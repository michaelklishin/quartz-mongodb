(ns com.novemberain.quartz.mongodb.cluster.scheduler-test
  (:require [clojure.test :refer :all])
  (:import com.novemberain.quartz.mongodb.cluster.Scheduler))

(defn- ^Scheduler create-scheduler
  [lastCheckinTime checkinInterval]
  (Scheduler. "name" "id" lastCheckinTime checkinInterval))

(deftest should-tell-when-scheduler-is-defunct
  ;; Checkin time was within expected time frame:
  (is (not (.isDefunct (create-scheduler 0 0) 0)))
  (is (not (.isDefunct (create-scheduler 0 0) Scheduler/TIME_EPSILON)))
  (is (not (.isDefunct (create-scheduler 1 0) (+ 1 Scheduler/TIME_EPSILON))))
  (is (not (.isDefunct (create-scheduler 0 1) (+ 1 Scheduler/TIME_EPSILON))))
  (is (not (.isDefunct (create-scheduler 1 1) (+ 2 Scheduler/TIME_EPSILON))))
  ;; 1 tick after expected checkin time:
  (is (.isDefunct (create-scheduler 0 0) (+ 1 Scheduler/TIME_EPSILON)))
  (is (.isDefunct (create-scheduler 1 0) (+ 2 Scheduler/TIME_EPSILON)))
  (is (.isDefunct (create-scheduler 0 1) (+ 2 Scheduler/TIME_EPSILON)))
  (is (.isDefunct (create-scheduler 1 1) (+ 3 Scheduler/TIME_EPSILON))))
