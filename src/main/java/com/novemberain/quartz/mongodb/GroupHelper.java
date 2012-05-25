package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("unchecked")
public class GroupHelper {
  private DBCollection collection;
  private QueryHelper queryHelper;
  public static final String KEY_GROUP = "keyGroup";
  public static final String JOB_ID = "jobId";
  private static final DBObject FIELDS = new BasicDBObject(KEY_GROUP, 1);

  public GroupHelper(DBCollection collection, QueryHelper queryHelper) {
    this.collection = collection;
    this.queryHelper = queryHelper;
  }


  public Set<String> groupsThatMatch(GroupMatcher matcher) {
    return new HashSet<String>(this.collection.distinct(KEY_GROUP, queryHelper.matchingKeysConditionFor(matcher)));
  }

  public Set<String> allGroups() {
    return new HashSet<String>(this.collection.distinct(KEY_GROUP));
  }

  public List<String> groupsForJobId(ObjectId jobId) {
    return (List<String>)this.collection.distinct(KEY_GROUP, new BasicDBObject(JOB_ID, jobId));
  }
}
