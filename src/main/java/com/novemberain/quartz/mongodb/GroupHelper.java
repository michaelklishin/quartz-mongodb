package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.HashSet;
import java.util.Set;

public class GroupHelper {
  private DBCollection collection;
  private QueryHelper queryHelper;
  public static final String KEY_GROUP = "keyGroup";
  private static final DBObject FIELDS = new BasicDBObject(KEY_GROUP, 1);

  public GroupHelper(DBCollection collection, QueryHelper queryHelper) {
    this.collection = collection;
    this.queryHelper = queryHelper;
  }


  public Set<String> groupsThatMatch(GroupMatcher matcher) {
    DBCursor cursor = collection.find(queryHelper.matchingKeysConditionFor(matcher), FIELDS);
    HashSet<String> hs = new HashSet<String>();

    for(DBObject dbo : cursor) {
      hs.add((String) dbo.get(KEY_GROUP));
    }

    return hs;
  }
}
