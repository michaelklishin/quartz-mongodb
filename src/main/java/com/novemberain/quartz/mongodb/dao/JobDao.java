package com.novemberain.quartz.mongodb.dao;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.novemberain.quartz.mongodb.JobConverter;
import com.novemberain.quartz.mongodb.util.GroupHelper;
import com.novemberain.quartz.mongodb.util.Keys;
import com.novemberain.quartz.mongodb.util.QueryHelper;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.*;

import static com.novemberain.quartz.mongodb.util.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.util.Keys.toFilter;

public class JobDao {

    private final MongoCollection<Document> jobCollection;
    private final QueryHelper queryHelper;
    private final GroupHelper groupHelper;
    private final JobConverter jobConverter;

    public JobDao(MongoCollection<Document> jobCollection,
                  QueryHelper queryHelper, JobConverter jobConverter) {
        this.jobCollection = jobCollection;
        this.queryHelper = queryHelper;
        this.groupHelper = new GroupHelper(jobCollection, queryHelper);
        this.jobConverter = jobConverter;
    }

    public MongoCollection<Document> getCollection() {
        return jobCollection;
    }

    public DeleteResult clear() {
        return jobCollection.deleteMany(new Document());
    }

    public void createIndex() {
        jobCollection.createIndex(Keys.KEY_AND_GROUP_FIELDS, new IndexOptions().unique(true));
    }

    public void dropIndex() {
        jobCollection.dropIndex("keyName_1_keyGroup_1");
    }

    public boolean exists(JobKey jobKey) {
        return jobCollection.count(Keys.toFilter(jobKey)) > 0;
    }

    public Document getById(Object id) {
        return jobCollection.find(Filters.eq("_id", id)).first();
    }

    public Document getJob(Bson keyObject) {
        return jobCollection.find(keyObject).first();
    }

    public Document getJob(JobKey key) {
        return getJob(toFilter(key));
    }

    public int getCount() {
        return (int) jobCollection.count();
    }

    public List<String> getGroupNames() {
        return jobCollection.distinct(KEY_GROUP, String.class).into(new ArrayList<String>());
    }

    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) {
        Set<JobKey> keys = new HashSet<JobKey>();
        Bson query = queryHelper.matchingKeysConditionFor(matcher);
        for (Document doc : jobCollection.find(query).projection(Keys.KEY_AND_GROUP_FIELDS)) {
            keys.add(Keys.toJobKey(doc));
        }
        return keys;
    }

    public Collection<ObjectId> idsOfMatching(GroupMatcher<JobKey> matcher) {
        List<ObjectId> list = new ArrayList<ObjectId>();
        for (Document doc : findMatching(matcher)) {
            list.add(doc.getObjectId("_id"));
        }
        return list;
    }

    public void remove(Bson keyObject) {
        jobCollection.deleteMany(keyObject);
    }

    public boolean requestsRecovery(JobKey jobKey) {
        //TODO check if it's the same as getJobDataMap?
        Document jobDoc = getJob(jobKey);
        return jobDoc.getBoolean(JobConverter.JOB_REQUESTS_RECOVERY, false);
    }

    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        Document doc = getJob(jobKey);
        if (doc == null) {
            //Return null if job does not exist, per interface
            return null;
        }
        return jobConverter.toJobDetail(doc);
    }

    public ObjectId storeJobInMongo(JobDetail newJob, boolean replaceExisting) throws JobPersistenceException {
        JobKey key = newJob.getKey();

        Bson keyDbo = toFilter(key);
        Document job = jobConverter.toDocument(newJob, key);

        Document object = getJob(keyDbo);

        ObjectId objectId = null;
        if (object != null && replaceExisting) {
            jobCollection.replaceOne(keyDbo, job);
        } else if (object == null) {
            try {
                jobCollection.insertOne(job);
                objectId = job.getObjectId("_id");
            } catch (MongoWriteException e) {
                // Fine, find it and get its id.
                object = getJob(keyDbo);
                objectId = object.getObjectId("_id");
            }
        } else {
            objectId = object.getObjectId("_id");
        }

        return objectId;
    }

    private Collection<Document> findMatching(GroupMatcher<JobKey> matcher) {
        return groupHelper.inGroupsThatMatch(matcher);
    }
}
