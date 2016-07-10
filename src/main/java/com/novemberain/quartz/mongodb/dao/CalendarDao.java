package com.novemberain.quartz.mongodb.dao;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.novemberain.quartz.mongodb.util.SerialUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.quartz.Calendar;
import org.quartz.JobPersistenceException;

public class CalendarDao {

    static final String CALENDAR_NAME = "name";
    static final String CALENDAR_SERIALIZED_OBJECT = "serializedObject";

    private final MongoCollection<Document> calendarCollection;

    public CalendarDao(MongoCollection<Document> calendarCollection) {
        this.calendarCollection = calendarCollection;
    }

    public void clear() {
        calendarCollection.deleteMany(new Document());
    }

    public void createIndex() {
        calendarCollection.createIndex(
                Projections.include(CALENDAR_NAME),
                new IndexOptions().unique(true));
    }

    public MongoCollection<Document> getCollection() {
        return calendarCollection;
    }

    public int getCount() {
        return (int) calendarCollection.count();
    }

    public boolean remove(String name) {
        Bson searchObj = Filters.eq(CALENDAR_NAME, name);
        if (calendarCollection.count(searchObj) > 0) {
            calendarCollection.deleteMany(searchObj);
            return true;
        }
        return false;
    }

    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        if (calName != null) {
            Bson searchObj = Filters.eq(CALENDAR_NAME, calName);
            Document doc = calendarCollection.find(searchObj).first();
            if (doc != null) {
                Binary serializedCalendar = doc.get(CALENDAR_SERIALIZED_OBJECT, Binary.class);
                return SerialUtils.deserialize(serializedCalendar, Calendar.class);
            }
        }
        return null;
    }
    
    public void store(String name, Calendar calendar) throws JobPersistenceException {
        Document doc = new Document(CALENDAR_NAME, name)
                .append(CALENDAR_SERIALIZED_OBJECT, SerialUtils.serialize(calendar));
        calendarCollection.insertOne(doc);
    }
}
