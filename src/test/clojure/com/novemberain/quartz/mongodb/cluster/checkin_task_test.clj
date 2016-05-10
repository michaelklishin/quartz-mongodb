(ns com.novemberain.quartz.mongodb.cluster.checkin-task-test
  (:require [clojure.test :refer :all])
  (:import com.mongodb.MongoException
           com.novemberain.quartz.mongodb.cluster.CheckinTask
           com.novemberain.quartz.mongodb.dao.SchedulerDao))

(defn- ^SchedulerDao create-scheduler-dao
  [checkin-fn]
  ;; Dummy args are needed to fulfill super ctor needs:
  (proxy [SchedulerDao] [nil "sn" "in" 0 nil]
    (checkIn [] (checkin-fn))))

(defn- create-task
  [checkin-fn]
  (CheckinTask. (create-scheduler-dao checkin-fn)))

(deftest should-store-scheduler-data-to-checkin
  (let [checkin-counter (atom 0)
        checkin-task (create-task #(swap! checkin-counter inc))]
    (.run checkin-task)
    (is (= @checkin-counter 1))
    (.run checkin-task)
    (is (= @checkin-counter 2))))

(deftest should-stop-scheduler-when-hit-by-exception
  (let [error-counter (atom 0)
        checkin-fn #(throw (MongoException. "Checkin Error!"))
        checkin-task (create-task checkin-fn)]
    (.setErrorHandler checkin-task
                      (reify Runnable (run [_] (swap! error-counter inc))))
    (.run checkin-task)
    (is (= @error-counter 1))))
