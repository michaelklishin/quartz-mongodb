package com.novemberain.quartz.mongodb

import org.joda.time.DateTime
import org.quartz.Scheduler
import org.quartz.SchedulerException
import org.quartz.impl.StdSchedulerFactory

class QuartzHelper {

    /**
     * Creates standard properties with MongoDBJobStore.
     */
    static def Properties createProps() {
        def props = new Properties()
        props.setProperty("org.quartz.jobStore.class",
                "com.novemberain.quartz.mongodb.DynamicMongoDBJobStore")
        props.setProperty("org.quartz.jobStore.mongoUri",
                "mongodb://localhost:${MongoHelper.DEFAULT_MONGO_PORT}")
        //;; Often check for triggers to speed up collisions:
        props.setProperty("org.quartz.scheduler.idleWaitTime", "1000")
        props.setProperty("org.quartz.jobStore.dbName", "quartz_mongodb_test")
        props.setProperty("org.quartz.threadPool.threadCount", "1")
        props.setProperty("org.quartz.scheduler.skipUpdateCheck", "true")
        props.setProperty("org.quartz.plugin.triggHistory.class",
                "org.quartz.plugins.history.LoggingTriggerHistoryPlugin")
        props.setProperty("org.quartz.plugin.jobHistory.class",
                "org.quartz.plugins.history.LoggingJobHistoryPlugin")
        props
    }

    /**
     * Creates properties for clustered scheduler.
     */
    static def Properties createClusteredProps(String instanceName) {
        def props = createProps()
        props.setProperty("org.quartz.jobStore.isClustered", "true")
        props.setProperty("org.quartz.scheduler.instanceId", instanceName)
        props.setProperty("org.quartz.scheduler.instanceName", "test cluster")
        props
    }

    /**
     * Create a new Scheduler with the following properties.
     */
    static def Scheduler createScheduler(Properties properties) {
        def factory = new StdSchedulerFactory()
        factory.initialize(properties)
        def scheduler = factory.getScheduler()
        scheduler.start()
        scheduler
    }

    /**
     * Create and start the default scheduler.
     */
    static def Scheduler startDefaultScheduler() {
        createScheduler(createProps())
    }

    static def createClusteredScheduler(String instanceName) {
        createScheduler(createClusteredProps(instanceName))
    }

    /**
     * Creates a cluster of schedulers with given names.
     */
    static def List<Scheduler> createCluster(String... names) {
        names.collect { createClusteredScheduler(it) }
    }

    /**
     * Shutdown all schedulers in given cluster.
     */
    static def void shutdown(List<Scheduler> cluster) {
        cluster.each {
            try {
                it.shutdown()
            } catch (SchedulerException e) {
                e.printStackTrace()
            }
        }
    }

    static def Date inSeconds(int n) {
        DateTime.now().plusSeconds(n).toDate()
    }

    static def Date in2Months() {
        DateTime.now().plusMonths(2).toDate()
    }
}
