package com.novemberain.quartz.mongodb.cluster;

public class Scheduler {

    private final String name;
    private final String instanceId;
    private final long lastCheckinTime;
    private final long checkinInterval;

    public Scheduler(String name, String instanceId, long lastCheckinTime, long checkinInterval) {
        this.name = name;
        this.instanceId = instanceId;
        this.lastCheckinTime = lastCheckinTime;
        this.checkinInterval = checkinInterval;
    }

    public String getName() {
        return name;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public long getLastCheckinTime() {
        return lastCheckinTime;
    }

    public long getCheckinInterval() {
        return checkinInterval;
    }
}
