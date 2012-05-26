package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.QueryBuilder;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.List;

import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;

@SuppressWarnings("unchecked")
public class TriggerGroupHelper extends GroupHelper {
  public static final String JOB_ID = "jobId";

  public TriggerGroupHelper(DBCollection collection, QueryHelper queryHelper) {
    super(collection, queryHelper);
  }

  public List<String> groupsForJobId(ObjectId jobId) {
    return (List<String>)this.collection.distinct(KEY_GROUP, new BasicDBObject(JOB_ID, jobId));
  }

  public List<String> groupsForJobIds(Collection<ObjectId> ids) {
    return (List<String>)this.collection.distinct(KEY_GROUP, QueryBuilder.start(JOB_ID).in(ids).get());
  }
}
