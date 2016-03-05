(ns com.novemberain.quartz.mongodb.utils
  (:import com.novemberain.quartz.mongodb.dao.SchedulerDao
           com.novemberain.quartz.mongodb.util.Clock))

(defn ^Clock const-clock
  ([] (const-clock 0))
  ([currentTimeMillis]
   (proxy [Clock] []
     (millis [] currentTimeMillis))))

(defn ^Clock inc-clock
  ([] (inc-clock (atom 0)))
  ([counter]
   (proxy [Clock] []
     (millis [] (swap! counter inc)))))
