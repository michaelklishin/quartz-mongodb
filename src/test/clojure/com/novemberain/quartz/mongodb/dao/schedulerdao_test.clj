(ns com.novemberain.quartz.mongodb.dao.schedulerdao-test
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.mongo-helper :as mongo])
  (:import com.novemberain.quartz.mongodb.dao.SchedulerDao
           com.novemberain.quartz.mongodb.util.Clock
           org.bson.Document))

(use-fixtures :each mongo/purge-collections)

(defonce schedulerName "Test Scheduler")

(defonce instanceId "Test instance")

(defonce ^long clusterCheckinIntervalMillis 5000)

(defn ^Clock create-clock
  ([] (create-clock (atom 0)))
  ([counter]
   (proxy [Clock] []
     (millis [] (swap! counter inc)))))

(def ^Clock test-clock (create-clock))

(defn create-dao
  ([] (create-dao test-clock))
  ([clock]
   (SchedulerDao. (mongo/get-schedulers-coll)
                  schedulerName instanceId clusterCheckinIntervalMillis
                  clock)))

(deftest should-have-passed-collection
  (let [dao (create-dao)]
    (is (identical? (mongo/get-schedulers-coll) (.getCollection dao)))
    (is (=  (.-schedulerName dao) schedulerName))
    (is (= (.-instanceId dao) instanceId))
    (is (= (.-clusterCheckinIntervalMillis dao) clusterCheckinIntervalMillis))
    (is (identical? (.-clock dao) test-clock))))

;; scheduler name, instance name, last checkin time, checkin interval
;; primary key: scheduler name, instance name
(deftest should-add-new-sheduler-instance-to-collection
  (let [counter (atom 0)
        dao (create-dao (create-clock counter))]
    (.checkIn dao)
    (is (= @counter 1))
    (is (= (mongo/get-count :schedulers) 1))
    (let [entry (mongo/get-first :schedulers)]
      (is (= (get entry "schedulerName") schedulerName))
      (is (= (get entry "instanceId") instanceId))
      (is (= (get entry "checkinInterval") (.-clusterCheckinIntervalMillis dao)))
      (is (= (get entry "lastCheckinTime") @counter)))))
