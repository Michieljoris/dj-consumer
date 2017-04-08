(ns dj-consumer.job
  (:require
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]))

(defn dispatch [k & args] k)

(defmulti before
  "Called before first attempt at job" dispatch)
(defmulti run
  "Run job, fails if it throws an exception, otherwise is considered success" dispatch)
(defmulti after
  "Called after successful job or after doing max-attempts and still failing" dispatch)
(defmulti success
  "Called after a job is successful" dispatch)
(defmulti fail
  "Called after trying to run job max-attempts times and job's still failing"dispatch)
(defmulti error
  "Called if run throws an exception" dispatch)
(defmulti config
  "Return a map with overrides and extra options for the job
  Extra options are :max-run-time :max-attempts, destroy-failed-job" dispatch)

(defmethod before :default [_ job])
(defmethod run :default [k job _] (timbre/error "Implementation is missing for job " k))
(defmethod after :default [_ job])
(defmethod success :default [_ job])
(defmethod fail :default [_ job])
(defmethod error :default [_ job e])
(defmethod config :default [_ job])

;; (doseq [mm ['config 'before 'after 'success 'fail 'error 'run]] (ns-unmap *ns* mm))
;; (defn invoke-hook [method job & args]
;;   (apply method (into [(:name job) job] args)))

;; (invoke-hook run {:name "foo"} :foo)
