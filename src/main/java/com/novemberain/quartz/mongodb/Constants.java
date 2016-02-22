package com.novemberain.quartz.mongodb;

public interface Constants {

  String JOB_DESCRIPTION = "jobDescription";
  String JOB_CLASS = "jobClass";
  String JOB_DURABILITY = "durability";
  String JOB_DATA = "jobData";
  String TRIGGER_CALENDAR_NAME = "calendarName";
  String TRIGGER_DESCRIPTION = "description";
  String TRIGGER_END_TIME = "endTime";
  String TRIGGER_FINAL_FIRE_TIME = "finalFireTime";
  String TRIGGER_FIRE_INSTANCE_ID = "fireInstanceId";
  String TRIGGER_MISFIRE_INSTRUCTION = "misfireInstruction";
  String TRIGGER_NEXT_FIRE_TIME = "nextFireTime";
  String TRIGGER_PREVIOUS_FIRE_TIME = "previousFireTime";
  String TRIGGER_PRIORITY = "priority";
  String TRIGGER_START_TIME = "startTime";
  String TRIGGER_JOB_ID = "jobId";
  String TRIGGER_CLASS = "class";
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
