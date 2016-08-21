package com.novemberain.quartz.mongodb;

public interface Constants {

  String JOB_DATA = "jobData";
  String JOB_DATA_PLAIN = "jobDataPlain";
  String TRIGGER_NEXT_FIRE_TIME = "nextFireTime";
  String TRIGGER_JOB_ID = "jobId";
  String TRIGGER_STATE = "state";
  String LOCK_INSTANCE_ID = "instanceId";
  String LOCK_TIME = "time";

  String STATE_WAITING = "waiting";
  String STATE_DELETED = "deleted";
  String STATE_COMPLETE = "complete";
  String STATE_PAUSED = "paused";
  String STATE_PAUSED_BLOCKED = "pausedBlocked";
  String STATE_BLOCKED = "blocked";
  String STATE_ERROR = "error";

}
