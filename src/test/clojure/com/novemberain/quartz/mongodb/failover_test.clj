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
           org.bson.types.ObjectId
           com.novemberain.quartz.mongodb.Constants
           com.novemberain.quartz.mongodb.util.Keys))

(use-fixtures :each mongo/purge-collections)

(def ^CountDownLatch singleJobCounter (CountDownLatch. 1))

(j/defjob SampleJob
  [ctx]
  (println "Executing SampleJob")
  ;; (System/exit 0)
  (.countDown singleJobCounter)
  )

(defn create-job []
  (j/build
   (j/of-type SampleJob)
   (j/with-identity "job1" "g1")
   (j/request-recovery)))

(defn insert-scheduler []
  (mongo/add-scheduler {"instanceId" "dead-node",
                        "schedulerName" "test cluster",
                        "lastCheckinTime" 1462806352702,
                        "checkinInterval" 7500}))

(defn insert-job []
  (mongo/add-job {"_id" (ObjectId. "00000000ee78252adaba4534"),
                  "keyName" "job1",
                  "keyGroup" "g1",
                  "jobDescription" nil,
                  "jobClass" "com.novemberain.quartz.mongodb.failover_test.SampleJob",
                  "durability" false,
                  "requestsRecovery" true}))

(defn insert-trigger []
  (mongo/add-trigger {"state" "waiting",
                      "calendarName" nil,
                      "class" "org.quartz.impl.triggers.CalendarIntervalTriggerImpl",
                      "description" nil,
                      "endTime" nil,
                      "finalFireTime" nil,
                      "fireInstanceId" nil,
                      "jobId" (ObjectId. "00000000ee78252adaba4534"),
                      "keyName" "t1",
                      "keyGroup" "g1",
                      "misfireInstruction" (int 0),
                      "nextFireTime" (Date. 1462820483910),
                      "previousFireTime" (Date. 1462820479910),
                      "priority" (int 5),
                      "startTime"(Date. 1462820481910),
                      "repeatIntervalUnit" "SECOND",
                      "repeatInterval" (int 2),
                      "timesTriggered" (int 1)}))

(defn insert-trigger-lock []
  (mongo/add-lock {Keys/KEY_GROUP "g1",
                   Keys/KEY_NAME "t1",
                   Constants/LOCK_INSTANCE_ID "dead-node",
                   Constants/LOCK_TIME (Date. 1462820481910)}))

(deftest should-reexecute-trigger-from-failed-execution
  (insert-scheduler)
  (insert-job)
  (insert-trigger)
  (insert-trigger-lock)
  (let [cluster (quartz/create-cluster "single-node")]
    (.await singleJobCounter 5000 TimeUnit/SECONDS)
    (is (= 0 (.getCount singleJobCounter))
        (str "Was: " (.getCount singleJobCounter)))
    (.sleep TimeUnit/SECONDS 2)
    (quartz/shutdown cluster)))
