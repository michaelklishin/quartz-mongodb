(ns com.novemberain.quartz-mongodb.test.units-test
  (:require [com.novemberain.quartz-mongodb.test.helper :as h]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers  :as qt]
            [clojurewerkz.quartzite.jobs      :as qj]
            [clojurewerkz.quartzite.matchers  :as qm]
            [clojurewerkz.quartzite.schedule.simple :as s]
            [monger.collection :as mgc])
  (:use clojure.test)
  (:import org.quartz.simpl.SimpleClassLoadHelper
           com.mulesoft.quartz.mongo.MongoDBJobStore)
  )

(use-fixtures :each h/purge-quartz-store)

(qj/defjob NoOpJob
  [ctx]
  )

(def cl (SimpleClassLoadHelper.))

(defn make-store
  []
  (doto (MongoDBJobStore.)
    (.setInstanceName "quartz_mongodb_test")
    (.setDbName "quartz_mongodb_test")
    (.setAddresses "127.0.0.1")
    (.initialize cl nil)))

(deftest test-storing-jobs
  (let [store (make-store)
        job   (qj/build
               (qj/of-type NoOpJob)
               (qj/with-identity "test-storing-jobs" "tests"))]
    (are [coll] (is (= 0 (mgc/count coll)))
         "quartz_jobs"
         "quartz_triggers")
    (.storeJob store job false)
    (is (= 1 (mgc/count "quartz_jobs")))
    (is (= 1 (.getNumberOfJobs store)))))

(deftest test-storing-triggers-with-simple-schedule
  (let [store (make-store)
        desc  "just a trigger"
        job   (qj/build
               (qj/of-type NoOpJob)
               (qj/with-identity "test-storing-triggers1" "tests"))
        tr    (qt/build
               (qt/start-now)
               (qt/with-identity "test-storing-triggers1" "tests")
               (qt/with-description desc)
               (qt/for-job job)
               (qt/with-schedule (s/schedule
                                  (s/with-repeat-count 10)
                                  (s/with-interval-in-milliseconds 400))))]
    (are [coll] (is (= 0 (mgc/count coll)))
         "quartz_jobs"
         "quartz_triggers")
    (doto store
      (.storeJob job false)
      (.storeTrigger tr false))
    (is (= 1 (mgc/count "quartz_triggers")))
    (is (= 1 (.getNumberOfTriggers store)))
    (let [m (mgc/find-one-as-map "quartz_triggers" {"keyName" "test-storing-triggers1"
                                                    "keyGroup" "tests"})]
      (is m)
      (is (= desc (:description m)))
      (is (= 400 (:repeatInterval m)))
      (is (:startTime m))
      (is (:finalFireTime m))
      (is (nil? (:nextFireTime m)))
      (is (nil? (:endTime m)))
      (is (nil? (:previousFireTime m)))
      (is (mgc/find-map-by-id "quartz_jobs" (:jobId m)))
      (is (= 0 (:timesTriggered m))))))
