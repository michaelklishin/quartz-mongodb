package com.novemberain.quartz.mongodb;

import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
import com.novemberain.quartz.mongodb.trigger.TriggerConverter;
import com.novemberain.quartz.mongodb.util.Keys;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TriggerAndJobPersister {

    private static final Logger log = LoggerFactory.getLogger(TriggerAndJobPersister.class);

    private final TriggerDao triggerDao;
    private final JobDao jobDao;
    private TriggerConverter triggerConverter;

    public TriggerAndJobPersister(TriggerDao triggerDao, JobDao jobDao, TriggerConverter triggerConverter) {
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.triggerConverter = triggerConverter;
    }

    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        final Document doc = jobDao.getJob(jobKey);
        return triggerDao.getTriggersForJob(doc);
    }

    public boolean removeJob(JobKey jobKey) {
        Bson keyObject = Keys.toFilter(jobKey);
        Document item = jobDao.getJob(keyObject);
        if (item != null) {
            jobDao.remove(keyObject);
            triggerDao.removeByJobId(item.get("_id"));
            return true;
        }
        return false;
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        for (JobKey key : jobKeys) {
            removeJob(key);
        }
        return false;
    }

    public boolean removeTrigger(TriggerKey triggerKey) {
        Bson filter = Keys.toFilter(triggerKey);
        Document trigger = triggerDao.findTrigger(filter);
        if (trigger != null) {
            removeOrphanedJob(trigger);
            //TODO: check if can .deleteOne(filter) here
            triggerDao.remove(filter);
            return true;
        }
        return false;
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        //FIXME return boolean allFound = true when all removed
        for (TriggerKey key : triggerKeys) {
            removeTrigger(key);
        }
        return false;
    }

    public boolean removeTriggerWithoutNextFireTime(OperableTrigger trigger) {
        if (trigger.getNextFireTime() == null) {
            log.info("Removing trigger {} as it has no next fire time.", trigger.getKey());
            removeTrigger(trigger.getKey());
            return true;
        }
        return false;
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger)
            throws JobPersistenceException {
        OperableTrigger oldTrigger = triggerDao.getTrigger(triggerKey);
        if (oldTrigger == null) {
            return false;
        }

        if (!oldTrigger.getJobKey().equals(newTrigger.getJobKey())) {
            throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
        }

        removeOldTrigger(triggerKey);
        copyOldJobDataMap(newTrigger, oldTrigger);
        storeNewTrigger(newTrigger, oldTrigger);

        return true;
    }

    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
            throws JobPersistenceException {
        ObjectId jobId = jobDao.storeJobInMongo(newJob, false);

        log.debug("Storing job {} and trigger {}", newJob.getKey(), newTrigger.getKey());
        storeTrigger(newTrigger, jobId, false);
    }

    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
            throws JobPersistenceException {
        if (newTrigger.getJobKey() == null) {
            throw new JobPersistenceException("Trigger must be associated with a job. Please specify a JobKey.");
        }

        Document doc = jobDao.getJob(Keys.toFilter(newTrigger.getJobKey()));
        if (doc != null) {
            storeTrigger(newTrigger, doc.getObjectId("_id"), replaceExisting);
        } else {
            throw new JobPersistenceException("Could not find job with key " + newTrigger.getJobKey());
        }
    }

    private void copyOldJobDataMap(OperableTrigger newTrigger, OperableTrigger trigger) {
        // Copy across the job data map from the old trigger to the new one.
        newTrigger.getJobDataMap().putAll(trigger.getJobDataMap());
    }

    private boolean isNotDurable(Document job) {
        return !job.containsKey(JobConverter.JOB_DURABILITY) ||
                job.get(JobConverter.JOB_DURABILITY).toString().equals("false");
    }

    private boolean isOrphan(Document job) {
        return (job != null) && isNotDurable(job) && triggerDao.hasLastTrigger(job);
    }

    private void removeOldTrigger(TriggerKey triggerKey) {
        // Can't call remove trigger as if the job is not durable, it will remove the job too
        triggerDao.remove(triggerKey);
    }

    // If the removal of the Trigger results in an 'orphaned' Job that is not 'durable',
    // then the job should be removed also.
    private void removeOrphanedJob(Document trigger) {
        if (trigger.containsKey(Constants.TRIGGER_JOB_ID)) {
            // There is only 1 job per trigger so no need to look further.
            Document job = jobDao.getById(trigger.get(Constants.TRIGGER_JOB_ID));
            if (isOrphan(job)) {
                jobDao.remove(job);
            }
        } else {
            log.debug("The trigger had no associated jobs");
        }
    }

    private void storeNewTrigger(OperableTrigger newTrigger, OperableTrigger oldTrigger)
            throws JobPersistenceException {
        try {
            storeTrigger(newTrigger, false);
        } catch (JobPersistenceException jpe) {
            storeTrigger(oldTrigger, false);
            throw jpe;
        }
    }

    private void storeTrigger(OperableTrigger newTrigger, ObjectId jobId, boolean replaceExisting)
            throws JobPersistenceException {
        Document trigger = triggerConverter.toDocument(newTrigger, jobId);
        if (replaceExisting) {
            trigger.remove("_id");
            triggerDao.replace(newTrigger.getKey(), trigger);
        } else {
            triggerDao.insert(trigger, newTrigger);
        }
    }
}
