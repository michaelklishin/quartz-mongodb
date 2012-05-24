(ns com.novemberain.quartz-mongodb.test.units-test
  (:require [com.novemberain.quartz-mongodb.test.helper :as h]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers  :as qt]
            [clojurewerkz.quartzite.jobs      :as qj]
            [clojurewerkz.quartzite.matchers  :as qm]
            [clojurewerkz.quartzite.schedule.simple :as s]
            [clojurewerkz.quartzite.schedule.cron :as sc]
            [monger.collection :as mgc])
  (:use clojure.test
        [clj-time.core :only [months from-now]])
  (:import org.quartz.simpl.SimpleClassLoadHelper
           com.mulesoft.quartz.mongo.MongoDBJobStore)
  )

(use-fixtures :each h/purge-quartz-store)

(qj/defjob NoOpJob
  [ctx]
  )

(def cl (SimpleClassLoadHelper.))
(def jobs-collection "quartz_jobs")
(def triggers-collection "quartz_triggers")

(defn make-store
  []
  (doto (MongoDBJobStore.)
    (.setInstanceName "quartz_mongodb_test")
    (.setDbName "quartz_mongodb_test")
    (.setAddresses "127.0.0.1")
    (.initialize cl nil)))

(defn has-fields?
  [m & fields]
  (every? #(m %) fields))

(defn nil-fields?
  [m & fields]
  (every? #(nil? (m %)) fields))

;;
;; Tests
;;

(deftest test-storing-jobs
  (let [store (make-store)
        job   (qj/build
               (qj/of-type NoOpJob)
               (qj/with-identity "test-storing-jobs" "tests"))]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (.storeJob store job false)
    (is (= 1 (mgc/count jobs-collection) (.getNumberOfJobs store)))))


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
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJob job false)
      (.storeTrigger tr false))
    (is (= 1
           (mgc/count jobs-collection)
           (mgc/count triggers-collection)
           (.getNumberOfTriggers store)))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-storing-triggers1"
                                                      "keyGroup" "tests"})]
      (is m)
      (is (= desc (:description m)))
      (is (= 400 (:repeatInterval m)))
      (is (= 0 (:timesTriggered m)))
      (is (has-fields? m :startTime :finalFireTime))
      (is (nil-fields? m :nextFireTime :endTime :previousFireTime))
      (is (mgc/find-map-by-id jobs-collection (:jobId m))))))


(deftest test-storing-triggers-with-cron-schedule
  (let [store (make-store)
        desc  "just a trigger that uses a cron expression schedule"
        job   (qj/build
               (qj/of-type NoOpJob)
               (qj/with-identity "test-storing-triggers2" "tests"))
        c-exp "0 0 15 L-1 * ?"
        tr    (qt/build
               (qt/start-now)
               (qt/with-identity "test-storing-triggers2" "tests")
               (qt/with-description desc)
               (qt/end-at (-> 2 months from-now))
               (qt/for-job job)
               (qt/with-schedule (sc/schedule
                                  (sc/cron-schedule c-exp))))]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJob job false)
      (.storeTrigger tr false))
    (Thread/sleep 100)
    (is (= 1
           (mgc/count jobs-collection)
           (mgc/count triggers-collection)
           (.getNumberOfTriggers store)))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-storing-triggers2"
                                                      "keyGroup" "tests"})]
      (is m)
      (is (= desc (:description m)))
      (is (= c-exp (:cronExpression m)))
      (is (has-fields? m :startTime :endTime :timezone))
      (is (nil-fields? m :nextFireTime :previousFireTime :repeatInterval :timesTriggered))
      (is (mgc/find-map-by-id jobs-collection (:jobId m))))))
