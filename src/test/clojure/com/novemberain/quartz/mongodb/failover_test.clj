(ns com.novemberain.quartz.mongodb.failover-test
  (:use [clj-time.core :only [seconds from-now]])
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.mongo-helper :as mongo]
            [com.novemberain.quartz.mongodb.quartz-helper :as quartz]
            [clojurewerkz.quartzite.jobs      :as j]
            [clojurewerkz.quartzite.triggers  :as t]
            [clojurewerkz.quartzite.schedule.simple :as s]
            [clojurewerkz.quartzite.schedule.calendar-interval :as calin])
  (:import java.util.Date
           [java.util.concurrent CountDownLatch TimeUnit]
           java.util.concurrent.atomic.AtomicInteger
           org.bson.types.ObjectId
           com.novemberain.quartz.mongodb.Constants
           com.novemberain.quartz.mongodb.util.Keys))

(use-fixtures :each mongo/purge-collections)

(defn insert-scheduler
  [id]
  (mongo/add-scheduler {"instanceId" id
                        "schedulerName" "test cluster"
                        "lastCheckinTime" 1462806352702
                        "checkinInterval" 7500}))

(defn insert-job
  [job-name]
  (mongo/add-job {"_id" (ObjectId. "00000000ee78252adaba4534")
                  "keyName" "job"
                  "keyGroup" "g1"
                  "jobDescription" nil
                  "jobClass" (str "com.novemberain.quartz.mongodb.failover_test." job-name)
                  "durability" false
                  "requestsRecovery" true}))

(def common-trigger-data {"state" "waiting"
                          "calendarName" nil
                          "description" nil
                          "endTime" nil
                          "fireInstanceId" nil
                          "jobId" (ObjectId. "00000000ee78252adaba4534")
                          "keyName" "t1"
                          "keyGroup" "g1"
                          "misfireInstruction" (int 0)
                          "nextFireTime" nil
                          "priority" (int 5)
                          "timesTriggered" (int 1)})

(defn insert-trigger []
  (mongo/add-trigger
   (merge common-trigger-data
          {"class" "org.quartz.impl.triggers.CalendarIntervalTriggerImpl"
           "finalFireTime" nil
           "nextFireTime" (Date. 1462820483910)
           "previousFireTime" (Date. 1462820479910)
           "startTime" (Date. 1462820481910)
           "repeatIntervalUnit" "SECOND"
           "repeatInterval" (int 2)})))

(defn insert-oneshot-trigger
  [fire-time]
  (mongo/add-trigger
   (merge common-trigger-data
          {"class" "org.quartz.impl.triggers.SimpleTriggerImpl"
           "finalFireTime" fire-time
           "nextFireTime" nil
           "previousFireTime" fire-time
           "startTime" fire-time
           "repeatCount" (int 0)
           "repeatInterval" (int 0)})))

(defn insert-trigger-lock
  [instance-id]
  (mongo/add-lock {Keys/KEY_GROUP "g1",
                   Keys/KEY_NAME "t1",
                   Constants/LOCK_INSTANCE_ID instance-id,
                   Constants/LOCK_TIME (Date. 1462820481910)}))

(def ^CountDownLatch job2-run-signaler (CountDownLatch. 1))

(j/defjob DeadJob2
  [ctx]
  (println "Executing DeadJob2")
  ;; (System/exit 0)
  (.countDown job2-run-signaler))

(deftest should-reexecute-other-trigger-from-failed-execution
  "Case: checks that time-based recurring trigger (has next fire time)
   from dead-node is picked up for execution."
  (insert-scheduler "dead-node")
  (insert-job "DeadJob2")
  (insert-trigger)
  (insert-trigger-lock "dead-node")
  (let [cluster (quartz/create-cluster "single-node")]
    (.await job2-run-signaler 2 TimeUnit/SECONDS)
    (is (= 0 (.getCount job2-run-signaler)))
    (.sleep TimeUnit/SECONDS 1)  ; let Quartz to finish job
    (quartz/shutdown cluster)))
