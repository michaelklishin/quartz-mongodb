## Changes between quartz-mongodb 1.9.0 and 2.0.0

### Clustering

Contributed by Przemysław Wojnowski.

### Java Upgrade

Quartz-mongodb is compiled with Java 7.

### MongoDB Java Driver Upgrade

MongoDB Java driver was upgraded to `3.2.x`.

Contributed by Przemysław Wojnowski.

### Joda Time Upgrade

Joda Time was upgraded to `2.4`.



## Changes between quartz-mongodb 1.8.0 and 1.9.0

### MongoDB Mocking

`MongoDBJobStore#overrideMongo` is a new method that allows a mock
implementation of MongoDB connection to be used, e.g. for integration
testing.

Contributed by Ben Romberg.


## Changes between quartz-mongodb 1.7.0 and 1.8.0

### Trigger Pause State is Respected

The store will now load only non-paused triggers.

Contributed by huang1900z.


## Changes between quartz-mongodb 1.6.0 and 1.7.0

### Configurable Job Timeout

Job timeout now can be configured via a property:

```
org.quartz.jobStore.jobTimeoutMillis=1800000
```

Contributed by David Regnier.


## Changes between quartz-mongodb 1.5.0 and 1.6.0

### More Robust MongoDBJobStore.getTriggersForJob

MongoDBJobStore.getTriggersForJob returns an empty list in case job
document does not exist (no longer throws as NPE).

Contributed by Illyr.

### MongoDB Java Driver Upgrade

MongoDB Java driver was upgraded to `2.12.1`.


## Changes between quartz-mongodb 1.4.0 and 1.5.0

### authDbName

`authDbName` is a new configurable property that makes
it possible to specify the database to authenticate
against.

Contributed by Maxim Markov.

### MongoDB Java Driver Upgrade

MongoDB Java driver was upgraded to `2.11.3`.

### MongoDB Client Deprecated APIs

(At least some) MongoDB Client Deprecated APIs are no longer used.

Contributed by lordbuddha.


## Changes between quartz-mongodb 1.3.0 and 1.4.0

### Switch to SLF4J Logging

Update the slf4j logging to use parametrized messages such that the
use of isDebugEnabled is not required.

Contributed by lordbuddha.

### Trigger Cleanup

If there is no next fire time, remove the trigger.  It is no longer
required.

Contributed by lordbuddha.

### Job Rescheduling Fix

When rescheduling a job, which uses replaceTrigger, do not use the
removeTrigger method as it will remove the job also if the job is not
durable.

Contributed by lordbuddha.

### Misfire Handling Fix

Fix misfire handling such that triggers whose next fire time after
misfire handling is outside of the current acquisition time are no
longer returned.

Contributed by lordbuddha.

### Batch Loading Support

Objects now can be loaded in batches, if Quartz is configured to do so.

Contributed by lordbuddha.

### Cluster Support Indicator

`MongoDBJobStore#isClustered` now correctly returns `false`.

Contributed by Dennis Zhuang.


## Changes between quartz-mongodb 1.2.0 and 1.3.0

### Connection Failures Throw Scheduler Exceptions

Connection failures will now result in Quartz scheduler exceptions
being thrown (as Quartz expects it to be), not MongoDB driver's.

### URI Connections

It is now possible to connect using a MongoDB URI:

``` ini
org.quartz.jobStore.class=com.novemberain.quartz.mongodb.MongoDBJobStore
# Use the mongo URI to connect
org.quartz.jobStore.mongoUri= mongodb://localhost:27020
org.quartz.jobStore.dbName=quartz
org.quartz.jobStore.collectionPrefix=mycol
org.quartz.threadPool.threadCount=100
```

### storeJobInMongo Behavior is More Compatible with the JDBC Store

JDBC store always stores a job that does not exist, regardless of the value of
the `replaceExisting` argument. Now MongoDB store does the same.

### Duplicate Triggers Issue is Fixed

Triggers now should be updated correctly.

### DailyTimeIntervalTriggers are Stored Successfully

DailyTimeIntervalTriggers are now deserialized correctly.



## Changes between quartz-mongodb 1.2.0-beta1 and 1.2.0

### Bug Fixes

Several improvements to make the store follow reference implementations
in Quartz more closely.

### Spring Code Cleanup

Multiple warnings and one string comparison bug fix were eliminated.


## Changes between quartz-mongodb 1.1.0 and 1.2.0-beta1

### Additional Indexes

The store will now create additional indexes on trigger fire times
to support cases with more than 64 MB of trigger documents.

GH issue: #13.


### Existing Jobs Now Can Be Replaced

`MongoDBJobStore` now supports replacing existing jobs.

GH issue: #20.

### Guards Against Misconfiguration

`MongoDBJobStore` constructor will now raise a `SchedulerConfigException`
if it fails to connect to MongoDB (typically because of a misconfiguration).

GH issue: #19.

### MongoDB Java Driver Upgrade

MongoDB Java driver was upgraded to `2.11.x`.


## Changes between quartz-mongodb 1.1.0-beta6 and 1.1.0

### MongoDB Java Driver Upgrade

MongoDB Java driver was upgraded to `2.10.x`.

### Quartz Upgrade

Quartz was upgraded to `2.1.7`.

### Joda Time Upgrade

Joda Time was upgraded to `2.2`.


## Changes between quartz-mongodb 1.1.0-beta5 and 1.1.0-beta6

Quartz-MongoDB now store instance id in lock documents.


## Changes between quartz-mongodb 1.1.0-beta4 and 1.1.0-beta5

Fixed a problem with JobExecutionContext.getPreviousFireTime() returning current execution time


## Changes between quartz-mongodb 1.1.0-beta3 and 1.1.0-beta4

Jobs that are not referenced by triggers are now cleaned up.


## Changes between quartz-mongodb 1.1.0-beta1 and 1.1.0-beta2

Added support for `getJobGroupNames` / `getTriggerGroupNames` in MongoDBJobStore.
