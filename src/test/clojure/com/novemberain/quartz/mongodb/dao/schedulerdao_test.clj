(ns com.novemberain.quartz.mongodb.dao.schedulerdao-test
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.mongo-helper :as mongo])
  (:import com.novemberain.quartz.mongodb.dao.SchedulerDao
           com.novemberain.quartz.mongodb.util.Clock))

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
  ([clock] (create-dao instanceId clock))
  ([id clock]
   (SchedulerDao. (mongo/get-schedulers-coll)
                  schedulerName id
                  clusterCheckinIntervalMillis
                  clock)))

(defn- check-scheduler
  ([dao entry expectedTimeMillis]
   (check-scheduler dao entry instanceId expectedTimeMillis))
  ([dao entry expectedId expectedTimeMillis]
   (is (= (get entry "schedulerName") schedulerName))
   (is (= (get entry "instanceId") expectedId))
   (is (= (get entry "checkinInterval") (.-clusterCheckinIntervalMillis dao)))
   (is (= (get entry "lastCheckinTime") expectedTimeMillis))))

(deftest should-have-passed-collection
  (let [dao (create-dao)]
    (is (identical? (mongo/get-schedulers-coll) (.getCollection dao)))
    (is (= (.-schedulerName dao) schedulerName))
    (is (= (.-instanceId dao) instanceId))
    (is (= (.-clusterCheckinIntervalMillis dao) clusterCheckinIntervalMillis))
    (is (identical? (.-clock dao) test-clock))))

;; scheduler name, instance name, last checkin time, checkin interval
;; primary key: scheduler name, instance name
(deftest should-add-new-entry-to-collection
  (let [counter (atom 0)
        dao (create-dao (create-clock counter))]
    (.checkIn dao)
    (is (= @counter 1))
    (is (= (mongo/get-count :schedulers) 1))
    (check-scheduler dao (mongo/get-first :schedulers) @counter)))

(deftest should-update-checkin-time
  (let [counter (atom 0)
        dao (create-dao (create-clock counter))]
    (.checkIn dao)
    (.checkIn dao)
    (is (= @counter 2))
    (is (= (mongo/get-count :schedulers) 1))
    (check-scheduler dao (mongo/get-first :schedulers) @counter)))

(deftest should-update-only-own-entry
  (let [id1 "id1" id2 "id2"
        id1-counter (atom 0)
        id2-counter (atom 100)
        dao1 (create-dao id1 (create-clock id1-counter))
        dao2 (create-dao id2 (create-clock id2-counter))]
    (.checkIn dao1)
    (.checkIn dao2)
    (is (= (mongo/get-count :schedulers) 2))
    (.checkIn dao2)
    (is (= @id2-counter 102))
    (let [entry1 (mongo/get-first :schedulers {"instanceId" id1})
          entry2 (mongo/get-first :schedulers {"instanceId" id2})]
      (check-scheduler dao1 entry1 id1 @id1-counter)
      (check-scheduler dao2 entry2 id2 @id2-counter))))

(deftest should-remove-selected-entry
  (let [id1 "id1" id2 "id2"
        dao (create-dao id1 test-clock)]
    ;; Create entries for two scheduler instances:
    (.checkIn dao)
    (.checkIn (create-dao id2 test-clock))
    (is (= (mongo/get-count :schedulers) 2))
    ;; Remove the first one:
    (.remove dao schedulerName id2)
    (is (= (mongo/get-count :schedulers) 1))
    (is (not (nil? (mongo/get-first :schedulers {"instanceId" id1}))))
    ;; Remove the last one:
    (.remove dao schedulerName id1)
    (is (= (mongo/get-count :schedulers) 0))))
