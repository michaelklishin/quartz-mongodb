(ns com.novemberain.quartz.mongodb.load-balancing-test
  (:use [clj-time.core :only [seconds from-now]])
  (:require [clojure.test :refer :all]
            [com.novemberain.quartz.mongodb.mongo-helper :as mongo]
            [com.novemberain.quartz.mongodb.quartz-helper :as quartz]
            [clojurewerkz.quartzite.jobs      :as j]
            [clojurewerkz.quartzite.triggers  :as t]
            [clojurewerkz.quartzite.schedule.simple :as s])
  (:import com.novemberain.quartz.mongodb.util.Keys))

(use-fixtures :each mongo/purge-collections)

(def counter (atom []))

(j/defjob SharedJob
  [ctx]
  (swap! counter conj
         (-> ctx .getScheduler .getSchedulerInstanceId))
  (Thread/sleep 2000))

(deftest should-execute-the-job-only-once
  (let [cluster (quartz/create-cluster "duch" "rysiek")
        job     (j/build
                 (j/of-type SharedJob)
                 (j/with-identity "job1" "g1"))
        trigger  (t/build
                  (t/start-at (-> 1 seconds from-now))
                  (t/with-identity "t1" "g1"))]
    (reset! counter [])
    (.scheduleJob (first cluster) job trigger)
    (Thread/sleep 7000)
    (is (= 1 (count @counter)) (str "Was: " @counter))
    (quartz/shutdown cluster)))
