package com.novemberain.quartz.mongodb;

import org.bson.Document;
import org.quartz.*;

import java.io.IOException;

import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.Keys.KEY_NAME;

public class JobLoader {

    private ClassLoader jobClassLoader;

    public JobLoader(ClassLoader jobClassLoader) {
        this.jobClassLoader = jobClassLoader;
    }

    public JobDetail loadJobDetail(Document doc) throws JobPersistenceException {
        try {
            // Make it possible for subclasses to use custom class loaders.
            // When Quartz jobs are implemented as Clojure records, the only way to use
            // them without switching to gen-class is by using a
            // clojure.lang.DynamicClassLoader instance.
            @SuppressWarnings("unchecked")
            Class<Job> jobClass = (Class<Job>) jobClassLoader
                    .loadClass((String) doc.get(Constants.JOB_CLASS));

            JobBuilder builder = createJobBuilder(doc, jobClass);
            withDurability(doc, builder);
            JobDataMap jobData = createJobDataMap(doc);
            return builder.usingJobData(jobData).build();
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Could not load job class " + doc.get(Constants.JOB_CLASS), e);
        } catch (IOException e) {
            throw new JobPersistenceException("Could not load job class " + doc.get(Constants.JOB_CLASS), e);
        }
    }

    private JobDataMap createJobDataMap(Document doc) throws IOException {
        JobDataMap jobData = new JobDataMap();

        String jobDataString = (String) doc.get(Constants.JOB_DATA);
        if (jobDataString != null) {
            SerialUtils.jobDataMapFromString(jobData, jobDataString);
        } else {
            for (String key : doc.keySet()) {
                if (!key.equals(KEY_NAME)
                        && !key.equals(KEY_GROUP)
                        && !key.equals(Constants.JOB_CLASS)
                        && !key.equals(Constants.JOB_DESCRIPTION)
                        && !key.equals(Constants.JOB_DURABILITY)
                        && !key.equals("_id")) {
                    jobData.put(key, doc.get(key));
                }
            }
        }

        jobData.clearDirtyFlag();
        return jobData;
    }

    private void withDurability(Document doc, JobBuilder builder) throws JobPersistenceException {
        Object jobDurability = doc.get(Constants.JOB_DURABILITY);
        if (jobDurability != null) {
            if (jobDurability instanceof Boolean) {
                builder.storeDurably((Boolean) jobDurability);
            } else if (jobDurability instanceof String) {
                builder.storeDurably(Boolean.valueOf((String) jobDurability));
            } else {
                throw new JobPersistenceException("Illegal value for " + Constants.JOB_DURABILITY + ", class "
                        + jobDurability.getClass() + " not supported");
            }
        }
    }

    private JobBuilder createJobBuilder(Document doc, Class<Job> jobClass) {
        return JobBuilder.newJob(jobClass)
                .withIdentity(doc.getString(KEY_NAME), doc.getString(KEY_GROUP))
                .withDescription(doc.getString(Constants.JOB_DESCRIPTION));
    }
}
