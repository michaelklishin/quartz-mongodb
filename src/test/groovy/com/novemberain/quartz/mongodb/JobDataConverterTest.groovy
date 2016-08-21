package com.novemberain.quartz.mongodb

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import org.bson.Document
import org.quartz.JobDataMap
import org.quartz.JobPersistenceException
import spock.lang.Specification

class JobDataConverterTest extends Specification {

    def converterBase64 = new JobDataConverter(true)
    def converterPlain = new JobDataConverter(false)

    def "empty job data doesn't modify the document"() {
        given:
        def emptyJobDataMap = new JobDataMap()
        def doc = new Document()
        when:
        converterBase64.toDocument(emptyJobDataMap, doc);
        then:
        doc.size() == 0
        when:
        converterPlain.toDocument(emptyJobDataMap, doc);
        then:
        doc.size() == 0
    }

    def "document without job data doesn't modify job data map"() {
        given:
        def doc = new Document()
        doc.put('foo', 'bar')
        doc.put('num', 123)
        def jobDataMap = new JobDataMap()
        when:
        def result1 = converterBase64.toJobData(doc, jobDataMap)
        then:
        !result1
        jobDataMap.size() == 0
        when:
        def result2 = converterPlain.toJobData(doc, jobDataMap)
        then:
        !result2
        jobDataMap.size() == 0
    }

    def "base64 encode works"() {
        given:
        def jobDataMap = createJobDataWithSerializableContent()
        def doc = new Document()
        when:
        converterBase64.toDocument(jobDataMap, doc)
        then:
        doc.size() == 1
        doc.get(Constants.JOB_DATA) == base64
    }

    def "base64 decode works"() {
        given:
        def doc = new Document()
        doc.put(Constants.JOB_DATA, base64)
        def jobDataMap = new JobDataMap()
        when:
        def result = converterBase64.toJobData(doc, jobDataMap)
        then:
        result
        jobDataMap.getWrappedMap().size() == 2
        jobDataMap.getWrappedMap() == createJobDataWithSerializableContent().getWrappedMap()
    }

    def "base64 decode fails"() {
        given:
        def doc = new Document()
        doc.put(Constants.JOB_DATA, 'a' + base64)
        def jobDataMap = new JobDataMap()
        when:
        converterBase64.toJobData(doc, jobDataMap)
        then:
        thrown(JobPersistenceException)
    }

    def "plain encode works"() {
        given:
        def jobDataMap = createJobDataWithSimpleContent()
        def doc = new Document()
        when:
        converterPlain.toDocument(jobDataMap, doc)
        then:
        doc.size() == 1
        doc.get(Constants.JOB_DATA_PLAIN) == createJobDataWithSimpleContent().getWrappedMap()
    }

    def "plain decode works"() {
        given:
        def doc = new Document()
        doc.put(Constants.JOB_DATA_PLAIN, createJobDataWithSimpleContent().getWrappedMap())
        def jobDataMap = new JobDataMap()
        when:
        def result = converterPlain.toJobData(doc, jobDataMap)
        then:
        result
        jobDataMap.getWrappedMap().size() == 2
        jobDataMap.getWrappedMap() == createJobDataWithSimpleContent().getWrappedMap()
    }

    def "plain decode falls back to base64"() {
        given:
        def doc = new Document()
        doc.put(Constants.JOB_DATA, base64)
        def jobDataMap = new JobDataMap()
        when:
        def result = converterPlain.toJobData(doc, jobDataMap)
        then:
        result
        jobDataMap.getWrappedMap().size() == 2
        jobDataMap.getWrappedMap() == createJobDataWithSerializableContent().getWrappedMap()
    }

    @ToString
    @EqualsAndHashCode
    static class Foo implements Serializable {
        Bar bar;
        String str;
    }

    @ToString
    @EqualsAndHashCode
    static class Bar implements Serializable {
        String str;
    }

    def base64 = 'rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAACdAADc3RydAADMTIzdAADZm9vc3IAN2NvbS5ub3ZlbWJlcmFpbi5xdWFydHoubW9uZ29kYi5Kb2JEYXRhQ29udmVydGVyVGVzdCRGb2+BqrZCje0rSgIAAkwAA2JhcnQAOUxjb20vbm92ZW1iZXJhaW4vcXVhcnR6L21vbmdvZGIvSm9iRGF0YUNvbnZlcnRlclRlc3QkQmFyO0wAA3N0cnQAEkxqYXZhL2xhbmcvU3RyaW5nO3hwc3IAN2NvbS5ub3ZlbWJlcmFpbi5xdWFydHoubW9uZ29kYi5Kb2JEYXRhQ29udmVydGVyVGVzdCRCYXL0UcKbkNKdkwIAAUwAA3N0cnEAfgAHeHB0AANhYmN0AANkZWZ4'

    def createJobDataWithSerializableContent() {
        def foo = new Foo(bar: new Bar(str: 'abc'), str: 'def')
        def map = [foo: foo, str: '123']
        new JobDataMap(map)
    }

    def createJobDataWithSimpleContent() {
        def map = [foo: 'foo', bar: [one: 1, two: 2.0, list: ['a', 'b', 'c']]]
        new JobDataMap(map)
    }
}
