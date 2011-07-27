A MongoDB JobStore for Quartz.

WARNING: not all functionality works. You may have to hack this if you use it.
On the bright side, there is a lot less to hack than if you didn't have this 
at all.

To configure, set your Quartz properties to something like this:

# Use the MongoDB store
org.quartz.jobStore.class=com.mulesoft.quartz.mongo.MongoDBJobStore
# comma separated list of mongodb hosts/replica set seeds
org.quartz.jobStore.addresses=host1,host2
# Mongo database name
org.quartz.jobStore.dbName=quartz
# Will be used to create collections like mycol_jobs, mycol_triggers, mycol_calendars, mycol_locks
org.quartz.jobStore.collectionPrefix=mycol
# not sure why Quartz requires this, but it does and we don't use it
org.quartz.threadPool.threadCount=1
        