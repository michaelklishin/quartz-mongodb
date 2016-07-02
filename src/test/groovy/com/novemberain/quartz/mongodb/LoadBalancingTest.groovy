package com.novemberain.quartz.mongodb

import org.quartz.Job
import org.quartz.JobBuilder
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.TriggerBuilder
import spock.lang.Specification

class LoadBalancingTest extends Specification {

    def setup() {
        MongoHelper.purgeCollections()
    }

    static counter = Collections.synchronizedList([])

    public static class SharedJob implements Job {
        @Override
        void execute(JobExecutionContext context) throws JobExecutionException {
            def id = context.getScheduler().getSchedulerInstanceId()
            println("Shared Job executed by: ${id}")
            counter.add(id)
            Thread.sleep(2000)
        }
    }

    def 'should execute the job only once'() {
        given:
        counter.clear()
        def cluster = QuartzHelper.createCluster('duch', 'rysiek')
        def job = JobBuilder.newJob()
                .ofType(SharedJob)
                .withIdentity('job1', 'g1')
                .build()
        def trigger = TriggerBuilder.newTrigger()
                .startAt(new Date(System.currentTimeMillis() + 1000l))
                .withIdentity('t1', 'g1')
                .build()

        when:
        cluster.first().scheduleJob(job, trigger)
        Thread.sleep(7000)

        then:
        QuartzHelper.shutdown(cluster)
        counter.size() == 1
    }
}