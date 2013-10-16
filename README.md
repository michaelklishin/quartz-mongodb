# A MongoDB-based store for Quartz.

This is a MongoDB-backed job store for the [Quartz scheduler](http://quartz-scheduler.org/).

## Maven Artifacts

Artifacts are released to [clojars.org](http://clojars.org).

If you are using Maven, add the following repository
definition to your `pom.xml`:

``` xml
<repository>
  <id>clojars.org</id>
  <url>http://clojars.org/repo</url>
</repository>
```

### The Most Recent Release

With Leiningen:

    [com.novemberain/quartz-mongodb "1.4.0-beta1"]


With Maven:

    <dependency>
      <groupId>com.novemberain</groupId>
      <artifactId>quartz-mongodb</artifactId>
      <version>1.4.0-beta1</version>
    </dependency>


## Usage

Set your Quartz properties to something like this:

    # Use the MongoDB store
    org.quartz.jobStore.class=com.novemberain.quartz.mongodb.MongoDBJobStore
    # MongoDB URI (optional if 'org.quartz.jobStore.addresses' is set)
    org.quartz.jobStore.mongoUri=mongodb://localhost:27020
    # comma separated list of mongodb hosts/replica set seeds (optional if 'org.quartz.jobStore.mongoUri' is set)
    org.quartz.jobStore.addresses=host1,host2
    # database name
    org.quartz.jobStore.dbName=quartz
    # Will be used to create collections like mycol_jobs, mycol_triggers, mycol_calendars, mycol_locks
    org.quartz.jobStore.collectionPrefix=mycol
    # thread count setting is ignored by the MongoDB store but Quartz requries it
    org.quartz.threadPool.threadCount=1


## Continuous Integration

[![Build Status](https://secure.travis-ci.org/michaelklishin/quartz-mongodb.png?branch=master)](http://travis-ci.org/michaelklishin/quartz-mongodb)

CI is hosted by [Travis CI](http://travis-ci.org/)


## License

[Apache Public License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)


## FAQ

### Project Origins

The project was originally started by MuleSoft. It supports all Quartz trigger types and
tries to be as feature complete as possible.

### Why the Fork?

MuleSoft developers did not respond to attempts to submit pull
requests for several months. As more and more functionality was added
and implementation code refactored, I decided to completely separate
this fork form GitHub forks network because the project is now too
different from the original one. All changes were made with respect to
the Apache Public License 2.0.
