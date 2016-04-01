(ns com.novemberain.quartz.mongodb.dao.schedulerdao-test
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.mongo-helper :as mongo]
            [com.novemberain.quartz.mongodb.utils :as utils])
  (:import com.novemberain.quartz.mongodb.dao.SchedulerDao
           com.novemberain.quartz.mongodb.util.Clock))

(use-fixtures :each mongo/purge-collections)

(defonce schedulerName "TestSched")

(defonce instanceId "TestID")

(defonce ^long clusterCheckinIntervalMillis 5000)

(def ^Clock test-clock (utils/inc-clock))

(defn- add-entry
  [id checkin-time]
  (mongo/add-scheduler {SchedulerDao/SCHEDULER_NAME_FIELD schedulerName
                        SchedulerDao/INSTANCE_ID_FIELD id
                        SchedulerDao/CHECKIN_INTERVAL_FIELD 100
                        SchedulerDao/LAST_CHECKIN_TIME_FIELD checkin-time}))

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
(deftest should-add-new-entry-to-collection
  (let [counter (atom 0)
        dao (create-dao (utils/inc-clock counter))]
    (.checkIn dao)
    (is (= @counter 1))
    (is (= (mongo/get-count :schedulers) 1))
    (check-scheduler dao (mongo/get-first :schedulers) @counter)))

(deftest should-update-checkin-time
  (let [counter (atom 0)
        dao (create-dao (utils/inc-clock counter))]
    (.checkIn dao)
    (.checkIn dao)
    (is (= @counter 2))
    (is (= (mongo/get-count :schedulers) 1))
    (check-scheduler dao (mongo/get-first :schedulers) @counter)))

(deftest should-update-only-own-entry
  (let [id1 "id1" id2 "id2"
        id1-counter (atom 0)
        id2-counter (atom 100)
        dao1 (create-dao id1 (utils/inc-clock id1-counter))
        dao2 (create-dao id2 (utils/inc-clock id2-counter))]
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
  (let [id1 "id1" id2 "id2" id3 "id3"
        dao (create-dao id1 test-clock)]
    ;; Create entries for two scheduler instances:
    (add-entry id1 1)
    (add-entry id2 2)
    (add-entry id3 3)
    ;; Remove non-existing does nothing:
    (is (false? (.remove dao "x-id" 1)))
    (is (= (mongo/get-count :schedulers) 3))
    (is (false? (.remove dao "id1" 4)))
    (is (= (mongo/get-count :schedulers) 3))
    ;; Remove the first one:
    (is (true? (.remove dao id2 2)))
    (is (= (map #(get % "instanceId") (mongo/find-all :schedulers))
           ["id1" "id3"]))
    ;; Remove the second one:
    (is (true? (.remove dao id1 1)))
    (is (= (map #(get % "instanceId") (mongo/find-all :schedulers))
           ["id3"]))
    ;; Remove the last one:
    (is (true? (.remove dao id3 3)))
    (is (= (mongo/get-count :schedulers) 0))))

(deftest should-return-empty-list-of-entries
  (let [dao (create-dao)
        schedulers (.getAllByCheckinTime dao)]
    (is (and (not (nil? schedulers))
             (empty? schedulers)))))

(deftest should-return-entries-ordered-by-checkin-time
  "Entities should be in ascending order by last checkin time."
  (add-entry "i1" 3)
  (add-entry "i2" 1)
  (add-entry "i3" 2)
  (let [dao (create-dao)
        schedulers (.getAllByCheckinTime dao)]
    (is (= (count schedulers) 3))
    (is (= #{schedulerName} (into #{} (map #(.getName %) schedulers))))
    (is (= #{100} (into #{} (map #(.getCheckinInterval %) schedulers))))
    (is (= '("i2" "i3" "i1") (map #(.getInstanceId %) schedulers)))
    (is (= '(1 2 3) (map #(.getLastCheckinTime %) schedulers)))))
