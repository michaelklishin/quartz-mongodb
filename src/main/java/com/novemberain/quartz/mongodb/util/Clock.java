package com.novemberain.quartz.mongodb.util;

/**
 * It's responsibility is to provide current time.
 */
public abstract class Clock {

    /**
     * Return current time in millis.
     */
    public abstract long millis();

    /**
     * Default implementation that returns system time.
     */
    public static final Clock SYSTEM_CLOCK = new Clock() {
        @Override
        public long millis() {
            return System.currentTimeMillis();
        }
    };
}
