package com.novemberain.quartz.mongodb

import com.novemberain.quartz.mongodb.util.Clock

import java.util.concurrent.atomic.AtomicInteger

class Clocks {

    static def Clock constClock() {
        constClock(0)
    }

    static def Clock constClock(long millis) {
        [millis: millis,
         now   : { new Date(millis) }] as Clock
    }

    static def Clock incClock() {
        incClock(new AtomicInteger(0))
    }

    static def Clock incClock(AtomicInteger counter) {
        [millis: { counter.incrementAndGet() },
         now   : { new Date(counter.incrementAndGet()) }] as Clock
    }
}
