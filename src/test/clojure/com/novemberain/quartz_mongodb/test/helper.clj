(ns com.novemberain.quartz-mongodb.test.helper
  (:require [monger.core :as mgc]
            [monger.collection :as mgcol]
            [clojurewerkz.quartzite.scheduler :as quartz])
  (:import com.mongodb.WriteConcern))

(mgc/connect!)
(mgc/set-db! (mgc/get-db "quartz_mongodb_test"))
(mgc/set-default-write-concern! WriteConcern/SAFE)

(quartz/initialize)
(quartz/start)

(defn purge-quartz-store
  [f]
  (let [rfn (fn []
              (mgcol/remove "quartz_triggers")
              (mgcol/remove "quartz_jobs")
              (mgcol/remove "quartz_locks")
              (mgcol/remove "quartz_calendars")
              (mgcol/remove "quartz_paused_trigger_groups")
              (mgcol/remove "quartz_paused_job_groups")
              (quartz/clear!))]
    (rfn)
    (f)
    (rfn)))
