package com.novemberain.quartz.mongodb;

import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.dao.PausedJobGroupsDao;
import com.novemberain.quartz.mongodb.dao.PausedTriggerGroupsDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
import com.novemberain.quartz.mongodb.util.GroupHelper;
import com.novemberain.quartz.mongodb.util.QueryHelper;
import com.novemberain.quartz.mongodb.util.TriggerGroupHelper;
import org.bson.types.ObjectId;
import org.quartz.JobKey;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class TriggerStateManager {

    private final TriggerDao triggerDao;
    private final JobDao jobDao;
    private PausedJobGroupsDao pausedJobGroupsDao;
    private final PausedTriggerGroupsDao pausedTriggerGroupsDao;
    private final QueryHelper queryHelper;

    public TriggerStateManager(TriggerDao triggerDao, JobDao jobDao,
                               PausedJobGroupsDao pausedJobGroupsDao,
                               PausedTriggerGroupsDao pausedTriggerGroupsDao,
                               QueryHelper queryHelper) {
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.pausedJobGroupsDao = pausedJobGroupsDao;
        this.pausedTriggerGroupsDao = pausedTriggerGroupsDao;
        this.queryHelper = queryHelper;
    }

    public Set<String> getPausedTriggerGroups() {
        return pausedTriggerGroupsDao.getPausedGroups();
    }

    public TriggerState getState(TriggerKey triggerKey) {
        return getTriggerState(triggerDao.getState(triggerKey));
    }

    public void pause(TriggerKey triggerKey) {
        triggerDao.setState(triggerKey, Constants.STATE_PAUSED);
    }

    public Collection<String> pause(GroupMatcher<TriggerKey> matcher) {
        triggerDao.setStateInMatching(matcher, Constants.STATE_PAUSED);

        final GroupHelper groupHelper = new GroupHelper(triggerDao.getCollection(), queryHelper);
        final Set<String> set = groupHelper.groupsThatMatch(matcher);
        pausedTriggerGroupsDao.pauseGroups(set);

        return set;
    }

    public void pauseAll() {
        final GroupHelper groupHelper = new GroupHelper(triggerDao.getCollection(), queryHelper);
        triggerDao.setStateInAll(Constants.STATE_PAUSED);
        pausedTriggerGroupsDao.pauseGroups(groupHelper.allGroups());
    }

    public void pauseJob(JobKey jobKey) {
        final ObjectId jobId = jobDao.getJob(jobKey).getObjectId("_id");
        final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerDao.getCollection(), queryHelper);
        List<String> groups = groupHelper.groupsForJobId(jobId);
        triggerDao.setStateByJobId(jobId, Constants.STATE_PAUSED);
        pausedTriggerGroupsDao.pauseGroups(groups);
    }

    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) {
        final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerDao.getCollection(), queryHelper);
        List<String> groups = groupHelper.groupsForJobIds(jobDao.idsOfMatching(groupMatcher));
        triggerDao.setStateInGroups(groups, Constants.STATE_PAUSED);
        pausedJobGroupsDao.pauseGroups(groups);
        return groups;
    }

    public void resume(TriggerKey triggerKey) {
        // TODO: port blocking behavior and misfired triggers handling from StdJDBCDelegate in Quartz
        triggerDao.setState(triggerKey, Constants.STATE_WAITING);
    }

    public Collection<String> resume(GroupMatcher<TriggerKey> matcher) {
        triggerDao.setStateInMatching(matcher, Constants.STATE_WAITING);

        final GroupHelper groupHelper = new GroupHelper(triggerDao.getCollection(), queryHelper);
        final Set<String> set = groupHelper.groupsThatMatch(matcher);
        pausedTriggerGroupsDao.unpauseGroups(set);
        return set;
    }

    public void resume(JobKey jobKey) {
        final ObjectId jobId = jobDao.getJob(jobKey).getObjectId("_id");
        // TODO: port blocking behavior and misfired triggers handling from StdJDBCDelegate in Quartz
        triggerDao.setStateByJobId(jobId, Constants.STATE_WAITING);
    }

    public void resumeAll() {
        final GroupHelper groupHelper = new GroupHelper(triggerDao.getCollection(), queryHelper);
        triggerDao.setStateInAll(Constants.STATE_WAITING);
        pausedTriggerGroupsDao.unpauseGroups(groupHelper.allGroups());
    }

    public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) {
        final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerDao.getCollection(), queryHelper);
        List<String> groups = groupHelper.groupsForJobIds(jobDao.idsOfMatching(groupMatcher));
        triggerDao.setStateInGroups(groups, Constants.STATE_WAITING);
        pausedJobGroupsDao.unpauseGroups(groups);
        return groups;
    }

    public void resetTriggerFromErrorState(TriggerKey triggerKey) {
        // Atomic updates cannot be done with the current model - across collections.
        String currentState = triggerDao.getState(triggerKey);
        if (!Constants.STATE_ERROR.equals(currentState)) {
            return;
        }
        String newState = Constants.STATE_WAITING;
        if (pausedTriggerGroupsDao.getPausedGroups().contains(triggerKey.getGroup())) {
            newState = Constants.STATE_PAUSED;
        }
        triggerDao.transferState(triggerKey, Constants.STATE_ERROR, newState);
    }

    private TriggerState getTriggerState(String value) {
        if (value == null) {
            return TriggerState.NONE;
        }

        if (value.equals(Constants.STATE_DELETED)) {
            return TriggerState.NONE;
        }

        if (value.equals(Constants.STATE_COMPLETE)) {
            return TriggerState.COMPLETE;
        }

        if (value.equals(Constants.STATE_PAUSED)) {
            return TriggerState.PAUSED;
        }

        if (value.equals(Constants.STATE_PAUSED_BLOCKED)) {
            return TriggerState.PAUSED;
        }

        if (value.equals(Constants.STATE_ERROR)) {
            return TriggerState.ERROR;
        }

        if (value.equals(Constants.STATE_BLOCKED)) {
            return TriggerState.BLOCKED;
        }

        // waiting or acquired
        return TriggerState.NORMAL;
    }
}
