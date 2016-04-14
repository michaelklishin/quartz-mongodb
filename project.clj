(defproject com.novemberain/quartz-mongodb "2.0.0-rc2-SNAPSHOT"
  :description "A MongoDB-backed store for Quartz Scheduler and Quartzite"
  :min-lein-version "2.4.2"
  :license {:name "Apache License 2.0"}
  :dependencies [[org.quartz-scheduler/quartz   "2.1.7"]
                 [org.mongodb/mongo-java-driver "3.2.2"]
                 [joda-time/joda-time           "2.8.2"]
                 [commons-codec/commons-codec   "1.10"]]
  :java-source-paths ["src/main/java"]
  :test-paths        ["src/test/clojure"]
  :test-selectors {:all     (constantly true)
                   :focus   :focus
                   :default (constantly true)}
  :javac-options     ["-target" "1.7" "-source" "1.7" "-Xlint:deprecation"]
  :profiles {:dev {:resource-paths ["src/test/resources"]
                   :dependencies [[org.clojure/clojure       "1.7.0"]
                                  [clojurewerkz/quartzite    "1.3.0"]
                                  [com.novemberain/monger    "1.7.0"]
                                  [org.clojure/tools.logging "0.3.1" :exclusions [org.clojure/clojure]]
                                  [org.slf4j/slf4j-simple    "1.7.10"]
                                  [org.slf4j/slf4j-api       "1.7.10"]]}
             :provided {:dependencies [[org.clojure/clojure       "1.7.0"]]}}
  :repositories {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}})
