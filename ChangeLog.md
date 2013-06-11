## Changes between quartz-mongodb 1.3.0-beta1 and 1.3.0

### Job Durability is Now Correctly Stored

Job durability attribute is now properly stored in the job document.

### Orphaned JobDetails Removed

JobDetail should no longer be orphaned after deleting the only trigger referencing it.


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
