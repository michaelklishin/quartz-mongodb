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
           com.novemberain.quartz.mongodb.util.Keys
           com.novemberain.quartz.mongodb.util.Keys$LockType))

(use-fixtures :each mongo/purge-collections)

(def ^long quartz-finish-waittime-secs 2)

(defn insert-scheduler
  [id]
  (mongo/add-scheduler {"instanceId" id
                        "schedulerName" "test cluster"
                        "lastCheckinTime" 1462806352702
                        "checkinInterval" 7500}))

(defn insert-job
  [job-name recover]
  (mongo/add-job {"_id" (ObjectId. "00000000ee78252adaba4534")
                  "keyName" "job"
                  "keyGroup" "g1"
                  "jobDescription" nil
                  "jobClass" (str "com.novemberain.quartz.mongodb.failover_test." job-name)
                  "durability" false
                  "requestsRecovery" recover}))

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
           "startTime" (Date. 1462820481910)
           "previousFireTime" (Date. 1462820481910)
           "nextFireTime" (Date. 1462820483910)
           "finalFireTime" nil
           "repeatIntervalUnit" "SECOND"
           "repeatInterval" (int 2)})))

(defn insert-simple-trigger
  "Inserts a simple trigger."
  ([fire-time repeat-interval repeat-count]
   (insert-simple-trigger fire-time nil repeat-interval repeat-count))
  ([fire-time final-fire-time repeat-interval repeat-count]
   (mongo/add-trigger
    (merge common-trigger-data
           {"class" "org.quartz.impl.triggers.SimpleTriggerImpl"
            "startTime" fire-time
            "previousFireTime" fire-time
            "nextFireTime" nil
            "finalFireTime" final-fire-time
            "repeatCount" (int repeat-count)
            "repeatInterval" repeat-interval}))))

(defn insert-oneshot-trigger
  "Inserts trigger that has no next fire time."
  [fire-time]
  (insert-simple-trigger fire-time 0 0))

(defn insert-trigger-lock
  [instance-id]
  (mongo/add-lock {Keys/LOCK_TYPE (.name Keys$LockType/t)
                   Keys/KEY_GROUP "g1"
                   Keys/KEY_NAME "t1"
                   Constants/LOCK_INSTANCE_ID instance-id
                   Constants/LOCK_TIME (Date. 1462820481910)}))

(j/defjob DeadJob1
  [ctx]
  (println "Executing DeadJob1")
  (throw (IllegalStateException. "Should not be executed!")))

(deftest should-execute-one-shot-trigger-only-once
  "Should remove own, one-shot triggers during startup.
This is needed, because otherwise there would be no way
to distinguish between own long-running jobs and own dead jobs."
  (insert-scheduler "single-node")
  (insert-job "DeadJob1" false)
  (insert-oneshot-trigger (Date. 1462820481910))
  (insert-trigger-lock "single-node")
  (let [cluster (quartz/create-cluster "single-node")]
    (.sleep TimeUnit/SECONDS quartz-finish-waittime-secs)
    (is (= (mongo/get-count :triggers) 0))
    (is (= (mongo/get-count :locks) 0))
    (quartz/shutdown cluster)))

(def ^CountDownLatch job2-run-signaler (CountDownLatch. 1))

(j/defjob DeadJob2
  [ctx]
  (println "Executing DeadJob2")
  (when (.isRecovering ctx)
    (throw (IllegalStateException. "Should not be in recovering state!")))
  (.countDown job2-run-signaler))

(deftest should-reexecute-other-trigger-from-failed-execution
  "Case: checks that time-based recurring trigger (has next fire time)
   from dead-node is picked up for execution."
  (insert-scheduler "dead-node")
  (insert-job "DeadJob2" false)
  (insert-trigger)
  (insert-trigger-lock "dead-node")
  (let [cluster (quartz/create-cluster "single-node")]
    (.await job2-run-signaler 2 TimeUnit/SECONDS)
    (is (= 0 (.getCount job2-run-signaler)))
    (.sleep TimeUnit/SECONDS quartz-finish-waittime-secs)  ; let Quartz finish job
    (quartz/shutdown cluster)))

(def ^CountDownLatch job3-run-signaler (CountDownLatch. 1))

(j/defjob DeadJob3
  [ctx]
  (println "Executing DeadJob3")
  (when-not (.isRecovering ctx)
    (throw (IllegalStateException. "Should be in recovering state!")))
  (.countDown job3-run-signaler))

(deftest should-recover-own-one-shot-trigger
  "Case: own, one-shot trigger whose job requests recovery
   should be run again."
  (insert-scheduler "single-node")
  (insert-job "DeadJob3" true)
  (insert-oneshot-trigger (Date. 1462820481910))
  (insert-trigger-lock "single-node")
  (let [cluster (quartz/create-cluster "single-node")]
    (.await job3-run-signaler 2 TimeUnit/SECONDS)
    (is (= 0 (.getCount job3-run-signaler)))
    (.sleep TimeUnit/SECONDS quartz-finish-waittime-secs)  ; let Quartz finish job
    (is (= (mongo/get-count :triggers) 0))
    (is (= (mongo/get-count :jobs) 0))
    (is (= (mongo/get-count :locks) 0))
    (quartz/shutdown cluster)))

(def ^CountDownLatch job4-run-signaler (CountDownLatch. 3))

(j/defjob DeadJob4
  [ctx]
  (println "Executing DeadJob4. Recovering:" (.isRecovering ctx)
           ", refire count:" (.getRefireCount ctx))
  (.countDown job4-run-signaler))

(deftest should-recover-own-repeating-trigger
  "Case: own, repeating trigger whose job requests recovery
   should be run times_left + 1 times."
  (insert-scheduler "single-node")
  (insert-job "DeadJob4" true)
  (insert-trigger-lock "single-node")
  (let [repeat-interval 1000
        repeat-count 2
        fire-time (Date.)
        final-fire-time (+ (.getTime fire-time)
                           (* repeat-interval (inc repeat-count)))]
    (insert-simple-trigger fire-time final-fire-time repeat-interval repeat-count))
  (let [cluster (quartz/create-cluster "single-node")]
    (.await job4-run-signaler 5 TimeUnit/SECONDS)
    (is (= 0 (.getCount job4-run-signaler)))
    (.sleep TimeUnit/SECONDS quartz-finish-waittime-secs)  ; let Quartz finish job
    (is (= (mongo/get-count :triggers) 0))
    (is (= (mongo/get-count :jobs) 0))
    (is (= (mongo/get-count :locks) 0))
    (quartz/shutdown cluster)))

(def ^CountDownLatch job5-run-signaler (CountDownLatch. 1))

(j/defjob DeadJob5
  [ctx]
  (println "Executing DeadJob5")
  (when-not (.isRecovering ctx)
    (throw (IllegalStateException. "Should be in recovering state!")))
  (.countDown job5-run-signaler))

(deftest should-recover-dead-node-one-shot-trigger
  "Case: other node's, one-shot trigger whose job requests recovery
   should be run again."
  (insert-scheduler "dead-node")
  (insert-job "DeadJob5" true)
  (insert-oneshot-trigger (Date. 1462820481910))
  (insert-trigger-lock "dead-node")
  (let [cluster (quartz/create-cluster "single-node")]
    (.await job5-run-signaler 2 TimeUnit/SECONDS)
    (is (= 0 (.getCount job5-run-signaler)))
    (.sleep TimeUnit/SECONDS quartz-finish-waittime-secs)  ; let Quartz finish job
    (is (= (mongo/get-count :triggers) 0))
    (is (= (mongo/get-count :jobs) 0))
    (is (= (mongo/get-count :locks) 0))
    (quartz/shutdown cluster)))

(def ^CountDownLatch job6-run-signaler (CountDownLatch. 3))

(j/defjob DeadJob6
  [ctx]
  (println "Executing DeadJob6. Recovering:" (.isRecovering ctx)
           ", refire count:" (.getRefireCount ctx))
  (.countDown job6-run-signaler))

(deftest should-recover-dead-node-repeating-trigger
  "Case: other node's, repeating trigger whose job requests recovery
   should be run times_left + 1 times."
  (insert-scheduler "dead-node")
  (insert-job "DeadJob6" true)
  (insert-trigger-lock "dead-node")
  (let [repeat-interval 1000
        repeat-count 2
        fire-time (Date.)
        final-fire-time (+ (.getTime fire-time)
                           (* repeat-interval (inc repeat-count)))]
    (insert-simple-trigger fire-time final-fire-time repeat-interval repeat-count))
  (let [cluster (quartz/create-cluster "single-node")]
    (.await job6-run-signaler 5 TimeUnit/SECONDS)
    (is (= 0 (.getCount job6-run-signaler)))
    (.sleep TimeUnit/SECONDS quartz-finish-waittime-secs)  ; let Quartz finish job
    (is (= (mongo/get-count :triggers) 0))
    (is (= (mongo/get-count :jobs) 0))
    (is (= (mongo/get-count :locks) 0))
    (quartz/shutdown cluster)))

(def ^CountDownLatch job7-run-signaler (CountDownLatch. 3))

(j/defjob DeadJob7
  [ctx]
  (println "Executing DeadJob7. Recovering:" (.isRecovering ctx)
           ", refire count:" (.getRefireCount ctx))
  (.countDown job7-run-signaler))

(deftest should-recover-dead-node-repeating-trigger-by-any-node
  "Case: other node's, repeating trigger whose job requests recovery
   should be run times_left + 1 times, when cluster consists of more
   than one node."
  (insert-scheduler "dead-node")
  (insert-job "DeadJob7" true)
  (insert-trigger-lock "dead-node")
  (let [repeat-interval 1000
        repeat-count 2
        fire-time (Date.)
        final-fire-time (+ (.getTime fire-time)
                           (* repeat-interval (inc repeat-count)))]
    (insert-simple-trigger fire-time final-fire-time repeat-interval repeat-count))
  (let [cluster (quartz/create-cluster "live-one" "live-two")]
    (.await job7-run-signaler 5 TimeUnit/SECONDS)
    (is (= 0 (.getCount job7-run-signaler)))
    (.sleep TimeUnit/SECONDS quartz-finish-waittime-secs)  ; let Quartz finish job
    (is (= (mongo/get-count :triggers) 0))
    (is (= (mongo/get-count :jobs) 0))
    (is (= (mongo/get-count :locks) 0))
    (quartz/shutdown cluster)))
