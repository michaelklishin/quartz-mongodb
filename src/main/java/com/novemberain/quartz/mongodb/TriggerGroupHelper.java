package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;

public class TriggerGroupHelper extends GroupHelper {
  public static final String JOB_ID = "jobId";

  public TriggerGroupHelper(MongoCollection<BasicDBObject> collection, QueryHelper queryHelper) {
    super(collection, queryHelper);
  }

  public List<String> groupsForJobId(ObjectId jobId) {
    return collection.distinct(KEY_GROUP, String.class)
            .filter(new BasicDBObject(JOB_ID, jobId))
            .into(new LinkedList<String>());
  }

  public List<String> groupsForJobIds(Collection<ObjectId> ids) {
    return collection.distinct(KEY_GROUP, String.class)
            .filter(Filters.in(JOB_ID, ids))
            .into(new LinkedList<String>());
  }
}
