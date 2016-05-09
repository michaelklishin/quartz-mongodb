(ns com.novemberain.quartz.mongodb.mongo-helper
  (:import com.mongodb.MongoClient
           com.mongodb.WriteConcern
           com.mongodb.client.MongoCollection
           com.mongodb.client.MongoDatabase
           org.bson.Document))

(defonce test-database-name "quartz_mongodb_test")

(defonce ^MongoClient client
  (let [c (MongoClient.)]
    (.setWriteConcern c WriteConcern/SAFE)
    c))

(defonce ^MongoDatabase test-database (.getDatabase client test-database-name))

(defonce collections
  {:calendars (.getCollection test-database "quartz_calendars")
   :locks (.getCollection test-database "quartz_locks")
   :jobs (.getCollection test-database "quartz_jobs")
   :job-groups (.getCollection test-database "quartz_paused_job_groups")
   :schedulers (.getCollection test-database "quartz_schedulers")
   :triggers (.getCollection test-database "quartz_triggers")
   :trigger-groups (.getCollection test-database "quartz_paused_trigger_groups")})

(defn- clear-coll
  [col-key]
  (.deleteMany (col-key collections) (Document.)))

(defn purge-collections
  "Remove all data from Quartz MongoDB collections."
  [f]
  (let [rfn (fn []
              (clear-coll :triggers)
              (clear-coll :jobs)
              (clear-coll :locks)
              (clear-coll :calendars)
              (clear-coll :schedulers)
              (clear-coll :trigger-groups)
              (clear-coll :job-groups))]
    (rfn) ; before test
    (f) ; calls tests
    (rfn) ; after test
    ))

(defn add-scheduler
  "Adds a new scheduler entry created from given map."
  [data-map]
  (.insertOne (:schedulers collections) (Document. data-map)))

(defn add-job
  "Adds a new Job entry created from given map."
  [data-map]
  (.insertOne (:jobs collections) (Document. data-map)))

(defn add-trigger
  "Adds a new Trigger entry created from given map."
  [data-map]
  (.insertOne (:triggers collections) (Document. data-map)))

(defn get-count
  "Return number of elements in a collection."
  [col-key]
  (.count (col-key collections)))

(defn get-locks-coll
  "Return locks collection as MongoCollection."
  []
  (:locks collections))

(defn get-schedulers-coll
  "Return schedulers collection as MongoCollection."
  []
  (:schedulers collections))

(defn get-first
  "Return the first document from given collection."
  ([col-key] (get-first col-key {}))
  ([col-key amap]
   (-> (col-key collections)
       (.find (Document. amap))
       (.first))))

(defn find-all
  "Return all documents from given collection."
  [col-key]
  (-> (col-key collections)
      (.find (Document.))
      (.into (java.util.LinkedList.))))
