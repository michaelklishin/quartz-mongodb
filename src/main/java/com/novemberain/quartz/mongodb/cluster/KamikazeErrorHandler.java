package com.novemberain.quartz.mongodb.cluster;

/**
 * This implementation pauses Quartz to not allow to execute the same JOB by two schedulers.
 *
 * If a scheduler cannot register itself due to an exception we stop JVM to prevent
 * concurrent execution of the same jobs together with other nodes that might have found this
 * scheduler as defunct and take over its triggers.
 */
public class KamikazeErrorHandler implements Runnable {
  @Override
  public void run() {
    System.exit(1);
  }
}
