(ns com.novemberain.quartz.mongodb.quartz-helper
  (:import java.util.Properties
           org.quartz.impl.StdSchedulerFactory))

(defn create-props
  "Creates standard properties with MongoDBJobStore."
  []
  (doto (Properties.)
    (.setProperty "org.quartz.jobStore.class"
                  "com.novemberain.quartz.mongodb.DynamicMongoDBJobStore")
    (.setProperty "org.quartz.jobStore.mongoUri"
                  "mongodb://localhost:27017")
    ;; Often check for triggers to speed up collisions:
    (.setProperty "org.quartz.scheduler.idleWaitTime" "1000")
    (.setProperty "org.quartz.jobStore.dbName" "quartz_mongodb_test")
    (.setProperty "org.quartz.threadPool.threadCount" "1")
    (.setProperty "org.quartz.scheduler.skipUpdateCheck" "true")
    (.setProperty "org.quartz.plugin.triggHistory.class"
                  "org.quartz.plugins.history.LoggingTriggerHistoryPlugin")
    (.setProperty "org.quartz.plugin.jobHistory.class"
                  "org.quartz.plugins.history.LoggingJobHistoryPlugin")))

(defn create-clustered-props
  "Creates properties for clustered scheduler."
  [^String instanceName]
  (doto (create-props)
    (.setProperty "org.quartz.jobStore.isClustered" "true")
    (.setProperty "org.quartz.scheduler.instanceId" instanceName)
    (.setProperty "org.quartz.scheduler.instanceName" "test cluster")))

(defn create-scheduler
  "Create a new Scheduler with the following properties."
  [^Properties properties]
  (let [factory (StdSchedulerFactory.)]
    (.initialize factory properties)
    (doto (.getScheduler factory)
      (.start))))

(defn create-clustered-scheduler
  [instanceName]
  (create-scheduler (create-clustered-props instanceName)))

(defn create-cluster
  "Creates a cluster of schedulers with given names."
  [& names]
  (doall (map create-clustered-scheduler names)))

(defn shutdown
  "Shutdown all schedulers in given cluster."
  [cluster]
  (doall (map #(.shutdown %) cluster)))
