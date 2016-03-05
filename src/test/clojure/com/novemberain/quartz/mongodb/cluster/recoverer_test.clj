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
  []
  ;; Dummy args are needed to fulfill super ctor needs:
  (proxy [SchedulerDao] [nil "sn" "in" 0 nil]
    (getAllByCheckinTime []
      [(create-scheduler "id1" 1)
       (create-scheduler "id2" 8000)])
    (remove [name instanceId lastCheckinTime]
      (assert (= name "name") (str "Was: " name))
      (assert (= instanceId "id1") (str "Was: " instanceId))
      (assert (= lastCheckinTime 1) (str "Was: " lastCheckinTime)))))

(deftest should-remove-defunct-schedulers
  (let [clock (utils/const-clock 9000)
        scheduler-dao (mock-scheduler-dao)
        recoverer (Recoverer. scheduler-dao clock)]
    (.recover recoverer)))
