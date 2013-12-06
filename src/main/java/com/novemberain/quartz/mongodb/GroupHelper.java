package com.novemberain.quartz.mongodb;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;

@SuppressWarnings("unchecked")
public class GroupHelper {
  protected DBCollection collection;
  protected QueryHelper queryHelper;

  public GroupHelper(DBCollection collection, QueryHelper queryHelper) {
    this.collection = collection;
    this.queryHelper = queryHelper;
  }

  public Set<String> groupsThatMatch(GroupMatcher<?> matcher) {
    return new HashSet<String>(this.collection.distinct(KEY_GROUP, queryHelper.matchingKeysConditionFor(matcher)));
  }

  public List<DBObject> inGroupsThatMatch(GroupMatcher<?> matcher) {
    return collection.find(QueryBuilder.start(KEY_GROUP).
        in(groupsThatMatch(matcher)).get()).
        toArray();
  }

  public Set<String> allGroups() {
    return new HashSet<String>(this.collection.distinct(KEY_GROUP));
  }

}
