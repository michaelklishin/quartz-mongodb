(ns com.novemberain.quartz.mongodb.cluster.recoverer-test
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.utils :as utils])
  (:import com.novemberain.quartz.mongodb.cluster.Recoverer
           com.novemberain.quartz.mongodb.cluster.Scheduler
           com.novemberain.quartz.mongodb.dao.SchedulerDao
           com.novemberain.quartz.mongodb.util.Clock))

(defn- ^Scheduler create-scheduler
  [id lastCheckinTime]
  (Scheduler. "name" id lastCheckinTime 0))

(defn- ^SchedulerDao mock-scheduler-dao
  [lastCheckinTime1 lastCheckinTime2 removals]
  ;; Dummy args are needed to fulfill super ctor needs:
  (proxy [SchedulerDao] [nil "sn" "in" 0 nil]
    (getAllByCheckinTime []
      [(create-scheduler "id1" lastCheckinTime1)
       (create-scheduler "id2" lastCheckinTime2)])
    (remove [name instanceId lastCheckinTime]
      (swap! removals conj {:name name
                            :instance-id instanceId
                            :last-checkin-time lastCheckinTime}))))

(deftest should-not-remove-active-schedulers
  (let [removals (atom [])
        clock (utils/const-clock 1)
        scheduler-dao (mock-scheduler-dao 1 2 removals)
        recoverer (Recoverer. scheduler-dao clock)]
    (.recover recoverer)
    (is (empty? @removals))))

(deftest should-remove-defunct-schedulers
  (let [removals (atom [])
        clock (utils/const-clock 10000)
        scheduler-dao (mock-scheduler-dao 1 10000 removals)
        recoverer (Recoverer. scheduler-dao clock)]
    (.recover recoverer)
    (is (= (count @removals) 1))
    (is (= (-> @removals first :name) "name"))
    (is (= (-> @removals first :instance-id) "id1"))
    (is (= (-> @removals first :last-checkin-time) 1))))
