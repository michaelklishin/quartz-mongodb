(ns com.novemberain.quartz.mongodb.dao.locksdao-test
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.mongo-helper :as mongo]
            [com.novemberain.quartz.mongodb.utils :as utils]
            [clojurewerkz.quartzite.triggers :as t])
  (:import com.novemberain.quartz.mongodb.dao.LocksDao
           com.novemberain.quartz.mongodb.util.Clock
           com.novemberain.quartz.mongodb.util.Keys
           com.novemberain.quartz.mongodb.util.Keys$LockType
           com.mongodb.MongoWriteException
           java.util.Date))

(use-fixtures :each mongo/purge-collections)

(def instanceId "locksDaoTestId")

(def ^Clock test-clock (utils/inc-clock))

(defn create-dao
  ([] (create-dao test-clock))
  ([clock] (create-dao clock instanceId))
  ([clock id]
   (LocksDao. (mongo/get-locks-coll) clock id)))

(defn- assert-lock
  ([lock instance-id time]
   (assert-lock lock "n1" "g1" instance-id time))
  ([lock key group instance-id time]
   (is (= (get lock Keys/LOCK_TYPE (.name Keys$LockType/t))))
   (is (= (get lock Keys/KEY_NAME) key))
   (is (= (get lock Keys/KEY_GROUP) group))
   (is (= (get lock "instanceId") instance-id))
   (is (= (.getTime (get lock "time")) time))))


(deftest should-have-passed-collection-and-instanceId
  (let [col (mongo/get-locks-coll)
        dao (LocksDao. col test-clock instanceId)]
    (is (= instanceId (.-instanceId dao)))
    (is (identical? col (.getCollection dao)))))

(deftest should-lock-trigger
  (let [clock (utils/inc-clock)
        dao (create-dao clock)]
    (.lockTrigger dao (t/key "n1" "g1"))
    (let [locks (mongo/find-all :locks)]
      (is (= 1 (count locks)))
      (assert-lock (first locks) instanceId 1))))

(deftest should-throw-exception-when-locking-trigger-again
  "Trigger should be no possibility to lock a trigger again."
  (let [dao (create-dao)]
    (.lockTrigger dao (t/key "n1" "g1"))
    (is (thrown? MongoWriteException
                 (.lockTrigger dao (t/key "n1" "g1"))))
    (let [locks (mongo/find-all :locks)]
      (is (= 1 (count locks)))
      (let [lock (first locks)]
        (is (= "n1" (get lock Keys/KEY_NAME)))
        (is (= "g1" (get lock Keys/KEY_GROUP)))))))

(deftest should-relock-trigger-when-found
  (let [counter (atom 0)
        clock (utils/inc-clock counter)
        other-id "defunct scheduler"]

    ;; Lock using other scheduler in time "1":
    (.lockTrigger (create-dao clock other-id)
                  (t/key "n1" "g1"))
    (let [locks (mongo/find-all :locks)
          lock (first locks)]
      (is (= (count locks) 1))
      (assert-lock lock other-id 1)
      (is (= @counter 1)))

    ;; Using the new scheduler relock lock with time "1":
    (is (true? (.relock (create-dao clock)
                        (t/key "n1" "g1")
                        (Date. @counter))))
    (let [locks (mongo/find-all :locks)
          lock (first locks)]
      ;; Still one lock, but with updated owner and time:
      (is (= (count locks) 1))
      (assert-lock lock instanceId 2)
      (is (= @counter 2)))))

(deftest should-not-relock-when-already-changed
  "Don't relock when other scheduler have already done that."
  (let [counter (atom 0)
        clock (utils/inc-clock counter)
        tkey (t/key "n1" "g1")
        other-id "defunct scheduler"
        other-dao (create-dao clock other-id)]

    ;; Lock using other scheduler in time "1":
    (.lockTrigger other-dao (t/key "n1" "g1"))

    ;; Update the lock with new time "2:
    (is (true? (.relock other-dao tkey (Date. @counter))))
    (is (= @counter 2))

    ;; Using the new scheduler try to relock with old time "1":
    (is (false? (.relock (create-dao clock) tkey
                         (Date. (dec @counter)))))

    (let [locks (mongo/find-all :locks)]
      ;; Still one lock, updated be previous scheduler:
      (is (= (count locks) 1))
      (assert-lock (first locks) other-id 2)
      (is (= @counter 3)))))
