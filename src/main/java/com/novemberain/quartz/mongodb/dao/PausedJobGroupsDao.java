package com.novemberain.quartz.mongodb.dao;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static com.novemberain.quartz.mongodb.util.Keys.KEY_GROUP;

public class PausedJobGroupsDao {

    private MongoCollection<Document> pausedJobGroupsCollection;

    public PausedJobGroupsDao(MongoCollection<Document> pausedJobGroupsCollection) {
        this.pausedJobGroupsCollection = pausedJobGroupsCollection;
    }

    public HashSet<String> getPausedGroups() {
        return pausedJobGroupsCollection.distinct(KEY_GROUP, String.class).into(new HashSet<String>());
    }

    public void pauseGroups(List<String> groups) {
        if (groups == null) {
            throw new IllegalArgumentException("groups cannot be null!");
        }
        List<Document> list = new ArrayList<Document>();
        for (String s : groups) {
            list.add(new Document(KEY_GROUP, s));
        }
        pausedJobGroupsCollection.insertMany(list);
    }

    public void remove() {
        pausedJobGroupsCollection.deleteMany(new Document());
    }

    public void unpauseGroups(Collection<String> groups) {
        pausedJobGroupsCollection.deleteMany(Filters.in(KEY_GROUP, groups));
    }
}
