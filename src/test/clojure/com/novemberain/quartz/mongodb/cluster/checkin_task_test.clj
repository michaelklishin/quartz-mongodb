(ns com.novemberain.quartz.mongodb.cluster.checkin-task-test
  (:use [clj-time.core :only [seconds from-now]])
  (:require [clojure.test :refer :all])
  (:import com.novemberain.quartz.mongodb.cluster.CheckinTask
           com.novemberain.quartz.mongodb.dao.SchedulerDao))

(defn- ^SchedulerDao create-scheduler-dao
  [checkin-counter]
  ;; Dummy args are needed to fulfill super ctor needs:
  (proxy [SchedulerDao] [nil "sn" "in" 0 nil]
    (checkIn [] (swap! checkin-counter inc))))

(defn- create-task
  [checkin-counter]
  (CheckinTask. (create-scheduler-dao checkin-counter)))

(deftest should-store-scheduler-data-to-checkin
  (let [checkin-counter (atom 0)
        checkinTask (create-task checkin-counter)]
    (.checkIn checkinTask)
    (is (= @checkin-counter 1))
    (.checkIn checkinTask)
    (is (= @checkin-counter 2))))
