package com.novemberain.quartz.mongodb;

import com.mongodb.DBObject;
import org.quartz.*;

import java.io.IOException;

import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.Keys.KEY_NAME;

public class JobLoader {

    private ClassLoader jobClassLoader;

    public JobLoader(ClassLoader jobClassLoader) {
        this.jobClassLoader = jobClassLoader;
    }

    public JobDetail loadJobDetail(DBObject dbObject) throws JobPersistenceException {
        try {
            // Make it possible for subclasses to use custom class loaders.
            // When Quartz jobs are implemented as Clojure records, the only way to use
            // them without switching to gen-class is by using a
            // clojure.lang.DynamicClassLoader instance.
            @SuppressWarnings("unchecked")
            Class<Job> jobClass = (Class<Job>) jobClassLoader
                    .loadClass((String) dbObject.get(Constants.JOB_CLASS));

            JobBuilder builder = createJobBuilder(dbObject, jobClass);
            withDurability(dbObject, builder);
            JobDataMap jobData = createJobDataMap(dbObject);
            return builder.usingJobData(jobData).build();
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Could not load job class " + dbObject.get(Constants.JOB_CLASS), e);
        } catch (IOException e) {
            throw new JobPersistenceException("Could not load job class " + dbObject.get(Constants.JOB_CLASS), e);
        }
    }

    private JobDataMap createJobDataMap(DBObject dbObject) throws IOException {
        JobDataMap jobData = new JobDataMap();

        String jobDataString = (String) dbObject.get(Constants.JOB_DATA);
        if (jobDataString != null) {
            SerialUtils.jobDataMapFromString(jobData, jobDataString);
        } else {
            for (String key : dbObject.keySet()) {
                if (!key.equals(KEY_NAME)
                        && !key.equals(KEY_GROUP)
                        && !key.equals(Constants.JOB_CLASS)
                        && !key.equals(Constants.JOB_DESCRIPTION)
                        && !key.equals(Constants.JOB_DURABILITY)
                        && !key.equals("_id")) {
                    jobData.put(key, dbObject.get(key));
                }
            }
        }

        jobData.clearDirtyFlag();
        return jobData;
    }

    private void withDurability(DBObject dbObject, JobBuilder builder) throws JobPersistenceException {
        Object jobDurability = dbObject.get(Constants.JOB_DURABILITY);
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

    private JobBuilder createJobBuilder(DBObject dbObject, Class<Job> jobClass) {
        return JobBuilder.newJob(jobClass)
                .withIdentity((String) dbObject.get(KEY_NAME), (String) dbObject.get(KEY_GROUP))
                .withDescription((String) dbObject.get(Constants.JOB_DESCRIPTION));
    }
}
