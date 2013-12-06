package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.utils.Key;

public class Keys {

  public static final String KEY_NAME = "keyName";
  public static final String KEY_GROUP = "keyGroup";

  // backwards compatibility...  Remove these in next 2.0 release.
  @Deprecated
  public static final String JOB_KEY_NAME = KEY_NAME;
  @Deprecated
  public static final String JOB_KEY_GROUP = KEY_GROUP;
  @Deprecated
  public static final String TRIGGER_KEY_NAME = KEY_NAME;
  @Deprecated
  public static final String TRIGGER_KEY_GROUP = KEY_GROUP;
  @Deprecated
  public static final String LOCK_KEY_NAME = KEY_NAME;
  @Deprecated
  public static final String LOCK_KEY_GROUP = KEY_GROUP;

  public static BasicDBObject keyToDBObject(Key<?> key) {
    BasicDBObject job = new BasicDBObject();
    job.put(KEY_NAME, key.getName());
    job.put(KEY_GROUP, key.getGroup());
    return job;
  }

  public static JobKey dbObjectToJobKey(DBObject dbo) {
    return new JobKey((String) dbo.get(KEY_NAME), (String) dbo.get(KEY_GROUP));
  }

  public static TriggerKey dbObjectToTriggerKey(DBObject dbo) {
    return new TriggerKey((String) dbo.get(KEY_NAME), (String) dbo.get(KEY_GROUP));
  }
}
