(ns com.novemberain.quartz.mongodb.lock-manager-test
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.utils :as utils]
            [clojurewerkz.quartzite.triggers :as t :only [key]])
  (:import com.mongodb.MongoWriteException
           com.mongodb.ServerAddress
           com.mongodb.WriteError
           com.novemberain.quartz.mongodb.LockManager
           com.novemberain.quartz.mongodb.dao.LocksDao
           com.novemberain.quartz.mongodb.util.ExpiryCalculator
           com.novemberain.quartz.mongodb.Constants
           java.util.Date
           org.bson.BsonDocument
           org.bson.Document))

(defn- mock-locks-dao
  [fn-map]
  (proxy [LocksDao] [nil (utils/const-clock) nil]
    (findTriggerLock [tkey]
      ((get fn-map :findTriggerLock (fn [_] nil)) tkey))
    (lockTrigger [tkey]
      ((get fn-map :lockTrigger (fn [_] nil)) tkey))
    (relock [tkey lock-time]
      ((get fn-map :relock (fn [a b] false)) tkey lock-time))))

(defn- mock-ecalc
  [check-fn]
  (proxy [ExpiryCalculator] [nil nil 0 0]
    (isTriggerLockExpired [lock]
      (check-fn lock))))

(defn- create-manager
  [^LocksDao locks-dao ^ExpiryCalculator expiry-calculator]
  (LockManager. locks-dao expiry-calculator))

(defn create-write-exception
  []
  (MongoWriteException. (WriteError. 42 "Just no!" (BsonDocument/parse "{}"))
                        (ServerAddress.)))

(deftest try-lock-should-lock-trigger-when-have-no-locks
  (let [tkey (t/key "n1" "g1")
        locks-dao (mock-locks-dao {:lockTrigger #(assert (identical? % tkey))})
        calculator nil
        manager (create-manager locks-dao calculator)]
    (is (true? (.tryLock manager tkey)))))

(deftest try-lock-cannot-get-existing-lock-for-expiration-check
  "LockDao.lockTrigger() throws exception and findTriggerLock() returns null."
  (let [tkey (t/key "n1" "g1")
        locks-dao (mock-locks-dao {:lockTrigger (fn [_] (throw (create-write-exception)))
                                   :findTriggerLock #(do (assert (identical? % tkey)) nil)})
        calculator nil
        manager (create-manager locks-dao calculator)]
    (is (false? (.relockExpired manager tkey)))))

(deftest should-not-relock-valid-lock
  (let [tkey (t/key "n1" "g1")
        existing-lock (Document.)
        locks-dao (mock-locks-dao {:lockTrigger (fn [_] (throw (create-write-exception)))
                                   :findTriggerLock (fn [_] existing-lock)})
        calculator (mock-ecalc #(do (assert (= existing-lock %)) false))
        manager (create-manager locks-dao calculator)]
    (is (false? (.relockExpired manager tkey)))))

(defn- check-relock
  [relock-result]
  (let [tkey (t/key "n1" "g1")
        lock-time (Date. 123)
        existing-lock (Document. Constants/LOCK_TIME lock-time)
        locks-dao (mock-locks-dao {:lockTrigger (fn [_] (throw (create-write-exception)))
                                   :findTriggerLock (fn [_] existing-lock)
                                   :relock (fn [tk time]
                                             (do (assert (= tkey tk))
                                                 (assert (= lock-time time))
                                                 relock-result))})
        calculator (mock-ecalc #(do (assert (= existing-lock %)) true))
        manager (create-manager locks-dao calculator)]
    (is (= (.relockExpired manager tkey) relock-result))))

(deftest should-relock-expired-lock
  (do (check-relock false)
      (check-relock true)))
