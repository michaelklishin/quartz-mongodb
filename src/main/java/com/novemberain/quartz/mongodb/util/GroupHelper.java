package com.novemberain.quartz.mongodb.util;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.novemberain.quartz.mongodb.util.Keys.KEY_GROUP;

public class GroupHelper {
    protected MongoCollection<Document> collection;
    protected QueryHelper queryHelper;

    public GroupHelper(MongoCollection<Document> collection, QueryHelper queryHelper) {
        this.collection = collection;
        this.queryHelper = queryHelper;
    }

    public Set<String> groupsThatMatch(GroupMatcher<?> matcher) {
        Bson filter = queryHelper.matchingKeysConditionFor(matcher);
        return collection
                .distinct(KEY_GROUP, String.class)
                .filter(filter)
                .into(new HashSet<String>());
    }

    public List<Document> inGroupsThatMatch(GroupMatcher<?> matcher) {
        return collection
                .find(Filters.in(KEY_GROUP, groupsThatMatch(matcher)))
                .into(new LinkedList<Document>());
    }

    public Set<String> allGroups() {
        return collection
                .distinct(KEY_GROUP, String.class)
                .into(new HashSet<String>());
    }
}
