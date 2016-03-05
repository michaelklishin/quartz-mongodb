package com.novemberain.quartz.mongodb.cluster;

import com.novemberain.quartz.mongodb.dao.SchedulerDao;
import com.novemberain.quartz.mongodb.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * The responsibility of this class is to recover defunct schedulers.
 *
 * It means that that it:
 * - finds defunct schedulers,
 * - takeovers their triggers' locks,
 * - removes the schedulers.
 *
 * Defunct schedulers are those that haven't do check-in at expected
 * "check-in time interval".
 */
public class Recoverer {

    private static final Logger log = LoggerFactory.getLogger(Recoverer.class);

    private SchedulerDao schedulerDao;
    private Clock clock;

    public Recoverer(SchedulerDao schedulerDao, Clock clock) {
        this.schedulerDao = schedulerDao;
        this.clock = clock;
    }

    public void recover() {
        for (Scheduler defunct : findDefunctSchedulers()) {
            takeoverTriggerLocks(defunct);
            remove(defunct);
        }
    }

    private void remove(Scheduler defunct) {
        // Remove so other instances won't try to recover it anymore:
        schedulerDao.remove(defunct.getName(), defunct.getInstanceId(),
                defunct.getLastCheckinTime());
    }

    private void takeoverTriggerLocks(Scheduler scheduler) {
        //TODO takeover trigger locks of defunct scheduler
    }

    private List<Scheduler> findDefunctSchedulers() {
        // Compare all schedulers using the same moment:
        final long now = clock.millis();

        List<Scheduler> forRecovery = new LinkedList<Scheduler>();
        for (Scheduler scheduler : schedulerDao.getAllByCheckinTime()) {
            if (scheduler.isDefunct(now)) {
                log.info("Found defunct scheduler: {}", scheduler);
                forRecovery.add(scheduler);
            }
        }
        return forRecovery;
    }
}
