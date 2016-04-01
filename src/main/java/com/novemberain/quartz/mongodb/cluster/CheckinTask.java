package com.novemberain.quartz.mongodb.cluster;

import com.mongodb.MongoException;
import com.novemberain.quartz.mongodb.dao.SchedulerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The responsibility of this class is to check-in inside Scheduler Cluster.
 */
public class CheckinTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(CheckinTask.class);

    private SchedulerDao schedulerDao;

    public CheckinTask(SchedulerDao schedulerDao) {
        this.schedulerDao = schedulerDao;
    }

    @Override
    public void run() {
        log.info("Node {}:{} checks-in.", schedulerDao.schedulerName, schedulerDao.instanceId);
        try {
            schedulerDao.checkIn();
        } catch (MongoException e) {
            //TODO what to do in case of errors? Stop Quartz?
            log.error("Node " + schedulerDao.instanceId + " could not check-in: " + e.getMessage(), e);
        }
    }
}
