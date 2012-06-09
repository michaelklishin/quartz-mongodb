package com.novemberain.quartz.mongodb;

public interface Constants {

  public static final String JOB_DESCRIPTION = "jobDescription";
  public static final String JOB_CLASS = "jobClass";
  public static final String TRIGGER_CALENDAR_NAME = "calendarName";
  public static final String TRIGGER_DESCRIPTION = "description";
  public static final String TRIGGER_END_TIME = "endTime";
  public static final String TRIGGER_FINAL_FIRE_TIME = "finalFireTime";
  public static final String TRIGGER_FIRE_INSTANCE_ID = "fireInstanceId";
  public static final String TRIGGER_MISFIRE_INSTRUCTION = "misfireInstruction";
  public static final String TRIGGER_NEXT_FIRE_TIME = "nextFireTime";
  public static final String TRIGGER_PREVIOUS_FIRE_TIME = "previousFireTime";
  public static final String TRIGGER_PRIORITY = "priority";
  public static final String TRIGGER_START_TIME = "startTime";
  public static final String TRIGGER_JOB_ID = "jobId";
  public static final String TRIGGER_CLASS = "class";
  public static final String TRIGGER_STATE = "state";
  public static final String CALENDAR_NAME = "name";
  public static final String CALENDAR_SERIALIZED_OBJECT = "serializedObject";
  public static final String LOCK_INSTANCE_ID = "instanceId";
  public static final String LOCK_TIME = "time";

  public static final String STATE_WAITING = "waiting";
  public static final String STATE_DELETED = "deleted";
  public static final String STATE_COMPLETE = "complete";
  public static final String STATE_PAUSED = "paused";
  public static final String STATE_PAUSED_BLOCKED = "pausedBlocked";
  public static final String STATE_BLOCKED = "blocked";
  public static final String STATE_ERROR = "error";

}