package com.novemberain.quartz.mongodb;

import com.novemberain.quartz.mongodb.util.SerialUtils;
import org.bson.Document;
import org.quartz.*;
import org.quartz.spi.ClassLoadHelper;

import java.io.IOException;

import static com.novemberain.quartz.mongodb.util.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.util.Keys.KEY_NAME;

public class JobConverter {

    public static final String JOB_DURABILITY = "durability";
    private static final String JOB_CLASS = "jobClass";
    private static final String JOB_DESCRIPTION = "jobDescription";
    public static final String JOB_REQUESTS_RECOVERY = "requestsRecovery";

    private ClassLoadHelper loadHelper;

    public JobConverter(ClassLoadHelper loadHelper) {
        this.loadHelper = loadHelper;
    }

    public Document toDocument(JobDetail newJob, JobKey key) {
        Document job = new Document();
        job.put(KEY_NAME, key.getName());
        job.put(KEY_GROUP, key.getGroup());
        job.put(JOB_DESCRIPTION, newJob.getDescription());
        job.put(JOB_CLASS, newJob.getJobClass().getName());
        job.put(JOB_DURABILITY, newJob.isDurable());
        job.put(JOB_REQUESTS_RECOVERY, newJob.requestsRecovery());
        job.putAll(newJob.getJobDataMap());
        return job;
    }

    public JobDetail toJobDetail(Document doc) throws JobPersistenceException {
        try {
            // Make it possible for subclasses to use custom class loaders.
            // When Quartz jobs are implemented as Clojure records, the only way to use
            // them without switching to gen-class is by using a
            // clojure.lang.DynamicClassLoader instance.
            @SuppressWarnings("unchecked")
            Class<Job> jobClass = (Class<Job>) loadHelper.getClassLoader()
                    .loadClass(doc.getString(JOB_CLASS));

            JobBuilder builder = createJobBuilder(doc, jobClass);
            withDurability(doc, builder);
            withRequestsRecovery(doc, builder);
            JobDataMap jobData = createJobDataMap(doc);
            return builder.usingJobData(jobData).build();
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Could not load job class " + doc.get(JOB_CLASS), e);
        } catch (IOException e) {
            throw new JobPersistenceException("Could not load job class " + doc.get(JOB_CLASS), e);
        }
    }

    private JobDataMap createJobDataMap(Document doc) throws IOException {
        JobDataMap jobData = new JobDataMap();

        String jobDataString = doc.getString(Constants.JOB_DATA);
        if (jobDataString != null) {
            jobData.putAll(SerialUtils.deserialize(jobData, jobDataString));
        } else {
            for (String key : doc.keySet()) {
                if (!key.equals(KEY_NAME)
                        && !key.equals(KEY_GROUP)
                        && !key.equals(JOB_CLASS)
                        && !key.equals(JOB_DESCRIPTION)
                        && !key.equals(JOB_DURABILITY)
                        && !key.equals(JOB_REQUESTS_RECOVERY)
                        && !key.equals("_id")) {
                    jobData.put(key, doc.get(key));
                }
            }
        }

        jobData.clearDirtyFlag();
        return jobData;
    }

    private void withDurability(Document doc, JobBuilder builder) throws JobPersistenceException {
        Object jobDurability = doc.get(JOB_DURABILITY);
        if (jobDurability != null) {
            if (jobDurability instanceof Boolean) {
                builder.storeDurably((Boolean) jobDurability);
            } else if (jobDurability instanceof String) {
                builder.storeDurably(Boolean.valueOf((String) jobDurability));
            } else {
                throw new JobPersistenceException("Illegal value for " + JOB_DURABILITY + ", class "
                        + jobDurability.getClass() + " not supported");
            }
        }
    }

    private void withRequestsRecovery(Document doc, JobBuilder builder) {
        if (doc.getBoolean(JOB_REQUESTS_RECOVERY, false)) {
            builder.requestRecovery(true);
        }
    }

    private JobBuilder createJobBuilder(Document doc, Class<Job> jobClass) {
        return JobBuilder.newJob(jobClass)
                .withIdentity(doc.getString(KEY_NAME), doc.getString(KEY_GROUP))
                .withDescription(doc.getString(JOB_DESCRIPTION));
    }
}
