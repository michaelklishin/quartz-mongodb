(ns com.novemberain.quartz-mongodb.test.units-test
  (:require [com.novemberain.quartz-mongodb.test.helper :as h]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs     :as j]
            [clojurewerkz.quartzite.matchers :as m]
            [clojurewerkz.quartzite.schedule.simple :as s]
            [clojurewerkz.quartzite.schedule.cron   :as sc]
            [clojurewerkz.quartzite.schedule.daily-interval    :as sd]
            [clojurewerkz.quartzite.schedule.calendar-interval :as calin]
            [monger.collection :as mgc]
            [clojure.test :refer :all]
            [clj-time.core :refer [secs months from-now]]
            [clj-time.coerce :refer [to-long]])
  (:import org.quartz.simpl.SimpleClassLoadHelper
           org.quartz.impl.triggers.SimpleTriggerImpl
           org.quartz.impl.JobDetailImpl
           com.novemberain.quartz.mongodb.MongoDBJobStore))

(use-fixtures :each h/purge-quartz-store)

(j/defjob NoOpJob
  [ctx]
  )

(defn make-no-op-job
  [name job-group]
  (j/build
   (j/of-type NoOpJob)
   (j/with-identity name job-group)))

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
        job   (make-no-op-job "test-storing-jobs" "tests")
        key (j/key "test-storing-jobs" "tests")]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (.storeJob store job false)
    (is (= 1 (mgc/count jobs-collection) (.getNumberOfJobs store)))
    (let [keys (.getJobKeys store (m/group-equals "tests"))
          f (first keys)]
      (is (= f key)))
    (let [keys (.getJobKeys store (m/group-equals "sabbra-cadabra"))]
      (is (empty? keys)))
    (let [keys (.getJobKeys store (m/group-starts-with "te"))
          f (first keys)]
      (is (= f key)))
    (let [keys (.getJobKeys store (m/group-ends-with "sts"))
          f (first keys)]
      (is (= f key)))
    (let [keys (.getJobKeys store (m/group-contains "es"))
          f (first keys)]
      (is (= f key)))))


(deftest test-storing-triggers-with-simple-schedule
  (let [store (make-store)
        desc  "just a trigger"
        job   (make-no-op-job "test-storing-triggers1" "tests")
        tk (t/key "test-storing-triggers1" "tests")
        tr (t/build
            (t/start-now)
            (t/with-identity tk)
            (t/with-description desc)
            (t/for-job job)
            (t/with-schedule (s/schedule
                               (s/with-repeat-count 10)
                               (s/with-interval-in-milliseconds 400))))
        key (t/key "test-storing-triggers1" "tests")]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJob job false)
      (.storeTrigger tr false))
    (is (= "NORMAL" (str (.getTriggerState store tk))))
    (is (= 1
           (mgc/count jobs-collection)
           (mgc/count triggers-collection)
           (.getNumberOfTriggers store)))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-storing-triggers1"
                                                      "keyGroup" "tests"})]
      (is m)
      (is (= "waiting" (:state m)))
      (is (= desc (:description m)))
      (is (= 400 (:repeatInterval m)))
      (is (= 0 (:timesTriggered m)))
      (is (has-fields? m :startTime :finalFireTime ))
      (is (nil-fields? m :nextFireTime :endTime :previousFireTime ))
      (is (mgc/find-map-by-id jobs-collection (:jobId m))))
    (let [keys (.getTriggerKeys store (m/group-equals "tests"))
          f (first keys)]
      (is (= f key)))
    (let [keys (.getTriggerKeys store (m/group-equals "sabbra-cadabra"))]
      (is (empty? keys)))
    (let [keys (.getTriggerKeys store (m/group-starts-with "te"))
          f (first keys)]
      (is (= f key)))
    (let [keys (.getTriggerKeys store (m/group-ends-with "sts"))
          f (first keys)]
      (is (= f key)))
    (let [keys (.getTriggerKeys store (m/group-contains "es"))
          f (first keys)]
      (is (= f key)))))


(deftest test-storing-triggers-with-cron-schedule
  (let [store (make-store)
        desc  "just a trigger that uses a cron expression schedule"
        job   (make-no-op-job "test-storing-triggers2" "tests")
        c-exp "0 0 15 L-1 * ?"
        tr (t/build
            (t/start-now)
            (t/with-identity "test-storing-triggers2" "tests")
            (t/with-description desc)
            (t/end-at (-> 2 months from-now))
            (t/for-job job)
            (t/with-schedule (sc/schedule
                               (sc/cron-schedule c-exp))))]
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
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-storing-triggers2"
                                                      "keyGroup" "tests"})]
      (is m)
      (is (= desc (:description m)))
      (is (= c-exp (:cronExpression m)))
      (is (has-fields? m :startTime :endTime :timezone ))
      (is (nil-fields? m :nextFireTime :previousFireTime :repeatInterval :timesTriggered ))
      (is (mgc/find-map-by-id jobs-collection (:jobId m))))))



(deftest test-storing-triggers-with-daily-interval-schedule
  (let [store (make-store)
        desc  "just a trigger that uses a daily interval schedule"
        job   (make-no-op-job "test-storing-triggers3" "tests")
        tr (t/build
            (t/start-now)
            (t/with-identity "test-storing-triggers3" "tests")
            (t/with-description desc)
            (t/end-at (-> 2 months from-now))
            (t/for-job job)
            (t/with-schedule (sd/schedule
                               (sd/every-day)
                               (sd/starting-daily-at (sd/time-of-day 9 00 00))
                               (sd/ending-daily-at (sd/time-of-day 18 00 00))
                               (sd/with-interval-in-hours 2))))]
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
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-storing-triggers3"
                                                      "keyGroup" "tests"})]
      (is m)
      (is (= 18 (get-in m [:endTimeOfDay :hour ])))
      (is (= 0 (get-in m [:endTimeOfDay :minute ])))
      (is (= 0 (get-in m [:endTimeOfDay :second ])))
      (is (= 9 (get-in m [:startTimeOfDay :hour ])))
      (is (= 0 (get-in m [:startTimeOfDay :minute ])))
      (is (= 0 (get-in m [:startTimeOfDay :second ])))
      (is (= desc (:description m)))
      (is (= 2 (:repeatInterval m)))
      (is (= "HOUR" (:repeatIntervalUnit m)))
      (is (has-fields? m :startTime :endTime ))
      (is (nil-fields? m :nextFireTime :previousFireTime ))
      (is (mgc/find-map-by-id jobs-collection (:jobId m))))))



(deftest test-storing-triggers-with-calendar-interval-schedule
  (let [store (make-store)
        desc  "just a trigger that uses a daily interval schedule"
        job   (make-no-op-job "test-storing-triggers4" "tests")
        tr (t/build
            (t/start-now)
            (t/with-identity "test-storing-triggers4" "tests")
            (t/with-description desc)
            (t/end-at (-> 2 months from-now))
            (t/for-job job)
            (t/with-schedule (calin/schedule
                               (calin/with-interval-in-hours 4))))]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJob job false)
      (.storeTrigger tr false))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-storing-triggers4"
                                                      "keyGroup" "tests"})]
      (is m)
      (is (= desc (:description m)))
      (is (= 4 (:repeatInterval m)))
      (is (= "HOUR" (:repeatIntervalUnit m)))
      (is (has-fields? m :startTime :endTime ))
      (is (nil-fields? m :nextFireTime :previousFireTime ))
      (is (mgc/find-map-by-id jobs-collection (:jobId m))))))


(deftest test-pause-trigger
  (let [store (make-store)
        job   (make-no-op-job"test-pause-trigger" "tests")
        tk (t/key "test-pause-trigger" "tests")
        tr (t/build
            (t/start-now)
            (t/with-identity "test-pause-trigger" "tests")
            (t/end-at (-> 2 months from-now))
            (t/for-job job)
            (t/with-schedule (calin/schedule
                               (calin/with-interval-in-hours 4))))]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJob job false)
      (.storeTrigger tr false))
    (.pauseTrigger store tk)
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-trigger"
                                                      "keyGroup" "tests"})]
      (is m)
      (is (= "PAUSED" (str (.getTriggerState store tk)))))
    (.resumeTrigger store tk)
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-trigger"
                                                      "keyGroup" "tests"})]
      (is (= "NORMAL" (str (.getTriggerState store tk)))))))


(deftest test-pause-triggers
  (let [store (make-store)
        job   (make-no-op-job "job-in-test-pause-triggers" "main-tests")
        tk1 (t/key "test-pause-triggers1" "main-tests")
        tr1 (t/build
             (t/start-now)
             (t/with-identity tk1)
             (t/end-at (-> 2 months from-now))
             (t/for-job job)
             (t/with-schedule (calin/schedule
                                (calin/with-interval-in-hours 4))))
        tk2 (t/key "test-pause-triggers2" "alt-tests")
        tr2 (t/build
             (t/start-now)
             (t/with-identity tk2)
             (t/for-job job)
             (t/with-schedule (s/schedule
                                (s/with-repeat-count 10)
                                (s/with-interval-in-milliseconds 400))))]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJob job false)
      (.storeTrigger tr1 false)
      (.storeTrigger tr2 false))
    (is (= 2 (mgc/count triggers-collection) (.getNumberOfTriggers store)))
    (.pauseTriggers store (m/group-starts-with "main"))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-triggers1"
                                                      "keyGroup" "main-tests"})]
      (is m)
      (is (= "paused" (:state m)))
      (is (= "PAUSED" (str (.getTriggerState store tk1)))))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-triggers2"
                                                      "keyGroup" "alt-tests"})]
      (is m)
      (is (= "waiting" (:state m)))
      (is (= "NORMAL" (str (.getTriggerState store tk2)))))
    (is (= #{"main-tests"} (.getPausedTriggerGroups store)))
    (.resumeTriggers store (m/group-ends-with "tests"))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-triggers1"
                                                      "keyGroup" "main-tests"})]
      (is (= "waiting" (:state m)))
      (is (= "NORMAL" (str (.getTriggerState store tk1)))))
    (is (empty? (.getPausedTriggerGroups store)))))

(deftest test-job-group-names
  (let [store (make-store)
        job1 (make-no-op-job "job-in-test-job-group-names" "test-job1")
        job2 (make-no-op-job "job-in-test-job-group-names" "test-job2")]
    (doto store
      (.storeJob job1 false)
      (.storeJob job2 false))

    (let [job-group-names (vec (.getJobGroupNames store))]
      (is (= 2 (count job-group-names)))
      (is (= "test-job1" (first job-group-names)))
      (is (= "test-job2" (second job-group-names))))))

(deftest test-trigger-group-names
  (let [store (make-store)
        job1 (make-no-op-job "job-in-test-trigger-group-names" "test-job1")
        tk1 (t/build
              (t/start-now)
              (t/with-identity "trigger-in-test-trigger-group-names" "test-trigger-1")
              (t/with-description "descroption")
              (t/end-at (-> 2 months from-now))
              (t/for-job job1)
              (t/with-schedule (calin/schedule
                                 (calin/with-interval-in-hours 4))))
        job2 (make-no-op-job "job-in-test-trigger-group-names" "test-job2")
        tk2  (t/build
               (t/start-now)
               (t/with-identity "trigger-in-test-trigger-group-names" "test-trigger-2")
               (t/with-description "descroption")
               (t/end-at (-> 2 months from-now))
               (t/for-job job2)
               (t/with-schedule (calin/schedule
                                  (calin/with-interval-in-hours 4))))]
    (doto store
      (.storeJob job1 false)
      (.storeTrigger tk1 false)
      (.storeJob job2 false)
      (.storeTrigger tk2 false))

    (let [trigger-group-names (vec (.getTriggerGroupNames store))]
      (is (= 2 (count trigger-group-names)))
      (is (= "test-trigger-1" (first trigger-group-names)))
      (is (= "test-trigger-2" (second trigger-group-names))))))

(deftest test-pause-all-triggers
  (let [store (make-store)
        job   (make-no-op-job "job-in-test-pause-all-triggers" "main-tests")
        tk1 (t/key "test-pause-all-triggers1" "main-tests")
        tr1 (t/build
             (t/start-now)
             (t/with-identity tk1)
             (t/end-at (-> 2 months from-now))
             (t/for-job job)
             (t/with-schedule (calin/schedule
                                (calin/with-interval-in-hours 4))))
        tk2 (t/key "test-pause-all-triggers2" "alt-tests")
        tr2 (t/build
             (t/start-now)
             (t/with-identity tk2)
             (t/for-job job)
             (t/with-schedule (s/schedule
                                (s/with-repeat-count 10)
                                (s/with-interval-in-milliseconds 400))))]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJob job false)
      (.storeTrigger tr1 false)
      (.storeTrigger tr2 false))
    (.pauseAll store)
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-all-triggers1"
                                                      "keyGroup" "main-tests"})]
      (is m)
      (is (= "paused" (:state m)))
      (is (= "PAUSED" (str (.getTriggerState store tk1)))))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-all-triggers2"
                                                      "keyGroup" "alt-tests"})]
      (is m)
      (is (= "waiting" (:state m)))
      (is (= "NORMAL" (str (.getTriggerState store tk2)))))
    (is (= #{"main-tests" "alt-tests"} (.getPausedTriggerGroups store)))
    (.resumeAll store)
    (is (empty? (.getPausedTriggerGroups store)))
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-all-triggers1"
                                                      "keyGroup" "main-tests"})]
      (is (= "waiting" (:state m)))
      (is (= "NORMAL" (str (.getTriggerState store tk1)))))))



(deftest test-pause-job
  (let [store (make-store)
        jk    (j/key "test-pause-job" "tests")
        job   (make-no-op-job "test-pause-job" "tests")
        tk (t/key "test-pause-job" "tests")
        tr (t/build
            (t/start-now)
            (t/with-identity "test-pause-job" "tests")
            (t/end-at (-> 2 months from-now))
            (t/for-job job)
            (t/with-schedule (calin/schedule
                               (calin/with-interval-in-hours 4))))]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJob job false)
      (.storeTrigger tr false))
    (.pauseJob store jk)
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-job"
                                                      "keyGroup" "tests"})]
      (is m)
      (is (= "PAUSED" (str (.getTriggerState store tk)))))
    (.resumeTrigger store tk)
    (let [m (mgc/find-one-as-map triggers-collection {"keyName" "test-pause-job"
                                                      "keyGroup" "tests"})]
      (is (= "NORMAL" (str (.getTriggerState store tk)))))))


(deftest test-pause-jobs
  (let [store (make-store)
        j1  (make-no-op-job "job-in-test-pause-jobs1" "main-tests")
        tk1 (t/key "test-pause-jobs1" "main-tests")
        tr1 (t/build
             (t/start-now)
             (t/with-identity tk1)
             (t/end-at (-> 2 months from-now))
             (t/for-job j1)
             (t/with-schedule (calin/schedule
                                (calin/with-interval-in-hours 4))))
        j2 (make-no-op-job "job-in-test-pause-jobs2" "alt-tests")
        tk2 (t/key "test-pause-jobs2" "alt-tests")
        tr2 (t/build
             (t/start-now)
             (t/with-identity tk2)
             (t/for-job j2)
             (t/with-schedule (s/schedule
                                (s/with-repeat-count 10)
                                (s/with-interval-in-milliseconds 400))))]
    (are [coll] (is (= 0 (mgc/count coll)))
         jobs-collection
         triggers-collection)
    (doto store
      (.storeJobAndTrigger j1 tr1)
      (.storeJobAndTrigger j2 tr2))
    (is (= 2 (mgc/count triggers-collection) (mgc/count jobs-collection) (.getNumberOfTriggers store) (.getNumberOfJobs store)))
    (.pauseJobs store (m/group-starts-with "main"))
    (let [m (mgc/find-one-as-map triggers-collection (array-map "keyName" "test-pause-jobs1"
                                                                "keyGroup" "main-tests"))]
      (is m)
      (is (= "paused" (:state m)))
      (is (= "PAUSED" (str (.getTriggerState store tk1)))))
    (let [m (mgc/find-one-as-map triggers-collection (array-map "keyName" "test-pause-jobs2"
                                                                "keyGroup" "alt-tests"))]
      (is m)
      (is (= "waiting" (:state m)))
      (is (= "NORMAL" (str (.getTriggerState store tk2)))))
    (is (empty? (.getPausedTriggerGroups store)))
    (is (= #{"main-tests"} (.getPausedJobGroups store)))
    (.resumeJobs store (m/group-ends-with "tests"))
    (let [m (mgc/find-one-as-map triggers-collection (array-map "keyName" "test-pause-jobs1"
                                                                "keyGroup" "main-tests"))]
      (is (= "waiting" (:state m)))
      (is (= "NORMAL" (str (.getTriggerState store tk1)))))
    (is (empty? (.getPausedTriggerGroups store)))))


(deftest test-aquire-next-trigger
  (let [store (make-store)
        j1  (make-no-op-job "job-in-test-aquire-next-trigger-job1" "main-tests")
        tk1 (t/key "test-aquire-next-trigger-trigger1" "main-tests")
        tr1 (t/build
             (t/start-at (-> 2 secs from-now))
             (t/with-identity tk1)
             (t/end-at (-> 2 months from-now))
             (t/for-job j1)
             (t/with-schedule (s/schedule
                               (s/with-repeat-count 2)
                               (s/with-interval-in-seconds 400))))

        j2  (make-no-op-job "job-in-test-aquire-next-trigger-job2" "main-tests")
        tk2 (t/key "test-aquire-next-trigger-trigger2" "main-tests")
        tr2 (t/build
             (t/start-at (-> 5 secs from-now))
             (t/with-identity tk2)
             (t/end-at (-> 2 months from-now))
             (t/for-job j2)
             (t/with-schedule (s/schedule
                               (s/with-repeat-count 2)
                               (s/with-interval-in-seconds 400))))

        j3  (make-no-op-job "job-in-test-aquire-next-trigger-job3" "main-tests")
        tk3 (t/key "test-aquire-next-trigger-trigger3" "main-tests")
        tr3 (t/build
             (t/start-at (-> 10 secs from-now))
             (t/with-identity tk3)
             (t/end-at (-> 2 months from-now))
             (t/for-job j3)
             (t/with-schedule (s/schedule
                               (s/with-repeat-count 2)
                               (s/with-interval-in-seconds 400))))]

    (.computeFirstFireTime tr1 nil)
    (.computeFirstFireTime tr2 nil)
    (.computeFirstFireTime tr3 nil)

    (doto store
      (.storeJob j1 false)
      (.storeTrigger tr1 false)
      (.storeJob j2 false)
      (.storeTrigger tr2 false)
      (.storeJob j3 false)
      (.storeTrigger tr3 false))

    (is (empty? (.acquireNextTriggers store 10 1 0)))

    (let [ff      (.. tr1 (getNextFireTime) (getTime))]
      (is (= tk1 (.. store (acquireNextTriggers (+ ff 10000) 1 0) (get 0) (getKey))))
      (is (= tk2 (.. store (acquireNextTriggers (+ ff 10000) 1 0) (get 0) (getKey))))
      (is (= tk3 (.. store (acquireNextTriggers (+ ff 10000) 1 0) (get 0) (getKey))))
      (is (empty? (.. store (acquireNextTriggers (+ ff 10000) 1 0)))))))
