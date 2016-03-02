package com.novemberain.quartz.mongodb.cluster;

import com.novemberain.quartz.mongodb.dao.SchedulerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The responsibility of this class is to check-in inside Scheduler Cluster.
 */
public class CheckinTask {

    private static final Logger log = LoggerFactory.getLogger(CheckinTask.class);

    private SchedulerDao schedulerDao;

    public CheckinTask(SchedulerDao schedulerDao) {
        this.schedulerDao = schedulerDao;
    }

    public void checkIn() {
        log.info("Node {}:{} checks-in.", schedulerDao.schedulerName, schedulerDao.instanceId);
        schedulerDao.checkIn();
    }

}
