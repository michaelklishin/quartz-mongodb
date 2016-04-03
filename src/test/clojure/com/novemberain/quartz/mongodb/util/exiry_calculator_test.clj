(ns com.novemberain.quartz.mongodb.load-balancing-test
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.utils :as utils])
  (:import com.novemberain.quartz.mongodb.Constants
           com.novemberain.quartz.mongodb.cluster.Scheduler
           com.novemberain.quartz.mongodb.dao.SchedulerDao
           com.novemberain.quartz.mongodb.util.Clock
           com.novemberain.quartz.mongodb.util.ExpiryCalculator
           java.util.Date
           org.bson.Document))

(def default-instance-id "test instance")

(defn ^Scheduler create-scheduler
  ([] (create-scheduler 100))
  ([last-checkin-time]
   (Scheduler. "sname" default-instance-id
               last-checkin-time
               100                      ; checkin-interval
               )))

(def ^long job-timeout-millis 100)
(def ^long trigger-timeout-millis 10000)

(defn- ^SchedulerDao create-scheduler-dao
  [scheduler]
  ;; Dummy args are needed to fulfill super ctor needs:
  (proxy [SchedulerDao] [nil "sn" "in" 0 nil]
    (findInstance [id] scheduler)))

(defn create-calc
  ([clock]
   (create-calc clock (create-scheduler)))
  ([clock scheduler]
   (ExpiryCalculator. (create-scheduler-dao scheduler)
                      clock
                      job-timeout-millis trigger-timeout-millis)))

(defn- create-doc
  ([lock-time]
   (create-doc default-instance-id lock-time))
  ([instance-id lock-time]
   (Document. {Constants/LOCK_TIME (Date. lock-time)
               Constants/LOCK_INSTANCE_ID instance-id})))

(deftest should-tell-if-job-lock-has-exired
  (let [clock (utils/const-clock 101)
        calc (create-calc clock)]
    ;; Expired lock: 101 - 0 > 100 (timeout)
    (is (true? (.isJobLockExpired calc (create-doc 0))))
    ;; Not expired: 101 - 1/101 <= 100
    (is (false? (.isJobLockExpired calc (create-doc 1))))
    (is (false? (.isJobLockExpired calc (create-doc 101))))))

(deftest should-tell-if-trigger-lock-has-expired
  (let [clock (utils/const-clock 10001)]
    ;; Tests for alive scheduler:
    (let [alive-scheduler (create-scheduler 5000) ; last-checkin-time = 5000
          calc (create-calc clock alive-scheduler)]
      ;; Expired lock: 10001 - 0 > 10000 (timeout)
      (is (false? (.isTriggerLockExpired calc (create-doc 0))))
      ;; Not expired: 101 - 1/10001 <= 10000
      (is (false? (.isTriggerLockExpired calc (create-doc 1))))
      (is (false? (.isTriggerLockExpired calc (create-doc 10001)))))
    ;; Tests for dead scheduler:
    (let [dead-scheduler (create-scheduler 0) ; last-checkin-time = 0
          calc (create-calc clock dead-scheduler)]
      ;; Expired lock: 10001 - 0 > 10000 (timeout)
      (is (true? (.isTriggerLockExpired calc (create-doc 0))))
      ;; Not expired: 10001 - 1/10001 <= 10000
      (is (false? (.isTriggerLockExpired calc (create-doc 1))))
      (is (false? (.isTriggerLockExpired calc (create-doc 10001)))))))
