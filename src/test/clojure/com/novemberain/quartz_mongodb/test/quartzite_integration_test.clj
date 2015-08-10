(ns com.novemberain.quartz-mongodb.test.quartzite-integration-test
  (:use clojure.test
        clojurewerkz.quartzite.conversion
        [clj-time.core :only [now seconds from-now]])
  (:require [clojurewerkz.quartzite.scheduler :as sched]
            [clojurewerkz.quartzite.jobs      :as j]
            [clojurewerkz.quartzite.triggers  :as t]
            [clojurewerkz.quartzite.matchers  :as m]
            [clojurewerkz.quartzite.schedule.simple :as s]
            [clojurewerkz.quartzite.schedule.calendar-interval :as calin])
  (:import java.util.concurrent.CountDownLatch
           org.quartz.impl.matchers.GroupMatcher))

;; These are integration tests from Quartzite.
;; They do a decent job of exercising most of the underlying store operations
;; so we just reuse it here.

(sched/initialize)
(sched/start)


;;
;; Case 1
;;

(def latch1 (CountDownLatch. 10))

(defrecord JobA []
  org.quartz.Job
  (execute [this ctx]
    (.countDown ^CountDownLatch latch1)))



(deftest test-basic-periodic-execution-with-a-job-defined-using-defrecord
  (is (sched/started?))
  (let [job     (j/build
                 (j/of-type JobA)
                 (j/with-identity "clojurewerkz.quartzite.test.execution.job1" "tests"))
        trigger  (t/build
                  (t/start-now)
                  (t/with-schedule (s/schedule
                                    (s/with-repeat-count 10)
                                    (s/with-interval-in-milliseconds 200))))]
    (sched/schedule job trigger)
    (let [j (sched/get-job (j/key "clojurewerkz.quartzite.test.execution.job1" "tests"))
          m (from-job-detail j)]
      (is j)
      (is (:key m))
      (is (nil? (:description m)))
      (is (:job-data m)))
    (.await ^CountDownLatch latch1)))



;;
;; Case 2
;;

(def counter2 (atom 0))

(j/defjob JobB
  [ctx]
  (swap! counter2 inc))

(deftest test-unscheduling-of-a-job-defined-using-defjob
  (is (sched/started?))
  (let [jk      (j/key "clojurewerkz.quartzite.test.execution.job2"     "tests")
        tk      (t/key "clojurewerkz.quartzite.test.execution.trigger2" "tests")
        job     (j/build
                 (j/of-type JobB)
                 (j/with-identity "clojurewerkz.quartzite.test.execution.job2" "tests"))
        trigger  (t/build
                  (t/start-now)
                  (t/with-identity "clojurewerkz.quartzite.test.execution.trigger2" "tests")
                  (t/with-description "just a trigger")
                  (t/with-schedule (s/schedule
                                    (s/with-repeat-count 10)
                                    (s/with-interval-in-milliseconds 400))))]
    (is (not (sched/all-scheduled? jk tk)))
    (is (not (sched/scheduled? jk)))
    (is (not (sched/scheduled? tk)))
    (sched/schedule job trigger)
    (is (sched/all-scheduled? jk tk))
    (is (sched/scheduled? jk))
    (is (sched/scheduled? tk))
    (is (not (empty? (sched/get-triggers [tk]))))
    (is (not (empty? (sched/get-jobs [jk]))))
    (let [t (sched/get-trigger tk)
          m (from-trigger t)]
      (is t)
      (is (:key m))
      (is (:description m))
      (is (:start-time m))
      (is (:next-fire-time m)))
    (is (sched/get-job jk))
    (is (nil? (sched/get-job (j/key "ab88fsyd7f" "k28s8d77s"))))
    (is (nil? (sched/get-trigger (t/key "ab88fsyd7f"))))
    (is (not (empty? (sched/get-trigger-keys (m/group-equals "tests")))))
    (is (not (empty? (sched/get-matching-triggers (m/group-equals "tests")))))
    (is (not (empty? (sched/get-job-keys (m/group-equals "tests")))))
    (is (not (empty? (sched/get-matching-jobs (m/group-equals "tests")))))
    (Thread/sleep 2000)
    (sched/unschedule-job tk)
    (is (not (sched/all-scheduled? tk jk)))
    (Thread/sleep 2000)
    (is (< @counter2 7))))



;;
;; Case 3
;;

(def counter3 (atom 0))

(j/defjob JobC
  [ctx]
  (swap! counter3 inc))

(deftest test-manual-triggering-of-a-job-defined-using-defjob
  (is (sched/started?))
  (let [jk      (j/key "clojurewerkz.quartzite.test.execution.job3" "tests")
        tk      (t/key "clojurewerkz.quartzite.test.execution.trigger3" "tests")
        job     (j/build
                 (j/of-type JobC)
                 (j/with-identity "clojurewerkz.quartzite.test.execution.job3" "tests"))
        trigger  (t/build
                  (t/start-now)
                  (t/with-identity "clojurewerkz.quartzite.test.execution.trigger3" "tests")
                  (t/with-schedule (s/schedule
                                    (s/with-repeat-count 10)
                                    (s/with-interval-in-seconds 2))))]
    (sched/schedule job trigger)
    (sched/trigger jk)
    (Thread/sleep 500)
    (is (= 2 @counter3))))


;;
;; Case 4
;;

(def value4 (atom nil))

(j/defjob JobD
  [ctx]
  (swap! value4 (fn [_]
                  (from-job-data (.getMergedJobDataMap ctx)))))

(deftest test-job-data-access
  (is (sched/started?))
  (let [jk      (j/key "clojurewerkz.quartzite.test.execution.job4" "tests")
        tk      (t/key "clojurewerkz.quartzite.test.execution.trigger4" "tests")
        job     (j/build
                 (j/of-type JobD)
                 (j/with-identity "clojurewerkz.quartzite.test.execution.job4" "tests")
                 (j/using-job-data { "job-key" "job-value" }))
        trigger  (t/build
                  (t/start-now)
                  (t/with-identity "clojurewerkz.quartzite.test.execution.trigger4" "tests")
                  (t/with-schedule (s/schedule
                                    (s/with-repeat-count 10)
                                    (s/with-interval-in-seconds 2))))]
    (sched/schedule job trigger)
    (sched/trigger jk)
    (Thread/sleep 1000)
    (is (= "job-value" (get @value4 "job-key")))))


;;
;; Case 5
;;

(def counter5 (atom 0))

(j/defjob JobE
  [ctx]
  (let [i (get (from-job-data ctx) "job-key")]
    (swap! counter5 + i)))

(deftest test-job-pausing-resuming-and-unscheduling
  (is (sched/started?))
  (let [jk      (j/key "clojurewerkz.quartzite.test.execution.job5" "tests.jobs.unscheduling")
        tk      (t/key "clojurewerkz.quartzite.test.execution.trigger5" "tests.jobs.unscheduling")
        job     (j/build
                 (j/of-type JobE)
                 (j/with-identity "clojurewerkz.quartzite.test.execution.job5" "tests.triggers.unscheduling")
                 (j/using-job-data { "job-key" 2 }))
        trigger  (t/build
                  (t/start-now)
                  (t/with-identity "clojurewerkz.quartzite.test.execution.trigger5" "tests.triggers.unscheduling")
                  (t/with-schedule (s/schedule
                                    (s/with-repeat-count 10)
                                    (s/with-interval-in-seconds 1))))]
    (sched/schedule job trigger)
    (sched/pause-job jk)
    (sched/resume-job jk)
    (sched/pause-jobs (GroupMatcher/groupEquals "tests.jobs.unscheduling"))
    (sched/resume-jobs (GroupMatcher/groupEquals "tests.jobs.unscheduling"))
    (sched/pause-trigger tk)
    (sched/resume-trigger tk)
    (sched/pause-triggers (GroupMatcher/groupEquals "tests.triggers.unscheduling"))
    (sched/resume-triggers (GroupMatcher/groupEquals "tests.triggers.unscheduling"))
    (sched/pause-all!)
    (sched/resume-all!)
    (sched/unschedule-job tk)
    (Thread/sleep 300)
    (sched/unschedule-jobs [tk])
    (sched/delete-job jk)
    (sched/delete-jobs [jk])
    (Thread/sleep 3000)
    ;; with start-now policty some executions
    ;; manages to get through. In part this test is supposed
    ;; to demonstrate it as much as test unscheduling/pausing functions. MK.
    (is (< @counter5 10))))


;;
;; Case 6
;;

(def latch6 (CountDownLatch. 3))

(j/defjob JobF
  [ctx]
  (.countDown ^CountDownLatch latch6))

(deftest test-basic-periodic-execution-with-calendar-interval-schedule
  (is (sched/started?))
  (let [job     (j/build
                 (j/of-type JobF)
                 (j/with-identity "clojurewerkz.quartzite.test.execution.job6" "tests"))
        trigger  (t/build
                  (t/start-now)
                  (t/with-schedule (calin/schedule
                                    (calin/with-interval-in-seconds 2))))]
    (sched/schedule job trigger)
    (.await ^CountDownLatch latch6)))



;;
;; Case 7
;;

(def counter7 (atom 0))

(j/defjob JobG
  [ctx]
  (swap! counter7 inc))

(deftest test-double-scheduling
  (is (sched/started?))
  (let [job     (j/build
                 (j/of-type JobG)
                 (j/with-identity "clojurewerkz.quartzite.test.execution.job7" "tests"))
        trigger  (t/build
                  (t/start-at (-> 2 seconds from-now))
                  (t/with-schedule (calin/schedule
                                    (calin/with-interval-in-seconds 2))))]
    (is (sched/schedule job trigger))
    ;; schedule will raise an exception
    (is (thrown?
         org.quartz.ObjectAlreadyExistsException
         (sched/schedule job trigger)))
    ;; but maybe-schedule will not
    (is (not (sched/maybe-schedule job trigger)))
    (is (not (sched/maybe-schedule job trigger)))
    (is (not (sched/maybe-schedule job trigger)))
    (Thread/sleep 7000)
    (is (= 3 @counter7))))
