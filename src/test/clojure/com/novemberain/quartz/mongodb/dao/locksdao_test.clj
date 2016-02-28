(ns com.novemberain.quartz.mongodb.dao.locksdao-test
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.mongo-helper :as mongo]
            [clojurewerkz.quartzite.triggers :as t])
  (:import com.novemberain.quartz.mongodb.dao.LocksDao
           com.novemberain.quartz.mongodb.util.Keys
           org.bson.Document))

(use-fixtures :each mongo/purge-collections)

(def instanceId "locksDaoTestId")

(defn create-dao []
  (LocksDao. (mongo/get-locks-coll) instanceId))

(deftest should-have-passed-collection-and-instanceId
  (let [col (mongo/get-locks-coll)
        dao (LocksDao. col instanceId)]
    (is (= instanceId (.-instanceId dao)))
    (is (identical? col (.getCollection dao)))))

(deftest should-lock-trigger
  (let [dao (create-dao)
        trigger (t/build
                 (t/with-identity (t/key "n1" "g1")))]
    (.lockTrigger dao trigger)
    (let [locks (mongo/find-all :locks)]
      (is (= 1 (count locks)))
      (let [lock (first locks)]
        (is (= "n1" (get lock Keys/KEY_NAME)))
        (is (= "g1" (get lock Keys/KEY_GROUP)))
        (is (= instanceId (get lock "instanceId")))))))
