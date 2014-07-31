(defproject com.novemberain/quartz-mongodb "1.10.0-SNAPSHOT"
  :description "A MongoDB-backed store for Quartz Scheduler and Quartzite"
  :min-lein-version "2.0.0"
  :license {:name "Apache License 2.0"}
  :dependencies [[org.quartz-scheduler/quartz   "2.1.7"]
                 [org.mongodb/mongo-java-driver "2.12.3"]
                 [joda-time/joda-time           "2.4"]]
  :java-source-paths ["src/main/java"]
  :test-paths        ["src/test/clojure"]
  :test-selectors {:all     (constantly true)
                   :focus   :focus
                   :default (constantly true)}
  :javac-options     ["-target" "1.6" "-source" "1.6"]
  :profiles {:dev {:resource-paths ["src/test/resources"]
                   :dependencies [[org.clojure/clojure       "1.5.1"]
                                  [clojurewerkz/quartzite    "1.2.0"]
                                  [com.novemberain/monger    "1.7.0"]
                                  [org.clojure/tools.logging "0.2.3" :exclusions [org.clojure/clojure]]
                                  [org.slf4j/slf4j-simple    "1.6.2"]
                                  [org.slf4j/slf4j-api       "1.6.2"]]}}
  :repositories {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}})
