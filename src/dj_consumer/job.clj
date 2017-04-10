(ns dj-consumer.job
  (:require
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]))

;; The various lifecycle multimethods for a job, dispatched ok name of the job.
;; Every job has a stop? key. This is an atom that's set to true if job times
;; out or worker is stopped. All appropriate lifecycle get called, regardless of
;; the value of stop? Take appropriate action in each.
(defn dispatch [k job] k)

;;Called when job is reserved.
(defmulti config
  "Return a map with overrides and extra options for the job
  Extra options are :max-run-time :max-attempts, destroy-failed-job" dispatch)

;;Start job lifecycle (in order)
(defmulti before
  "Called before first attempt at job" dispatch)

(defmulti run
  "Run job, fails if it throws an exception, otherwise is considered success" dispatch)

;;Either success or error gets called
(defmulti success
  "Called after a job is successful" dispatch)
(defmulti error
  "Called if run throws an exception, error is on job under error key" dispatch)

(defmulti after
  "Always called, success or error" dispatch)
;;End job lifecycle

(defmulti fail
  "Called after trying to run job max-attempts times and job's still failing" dispatch)

(defmethod config :default [_ job])
(defmethod before :default [_ job])
(defmethod run :default [k job] (timbre/error "Implementation is missing for job " k))
(defmethod success :default [_ job])
(defmethod error :default [_ job])
(defmethod after :default [_ job])
(defmethod fail :default [_ job])

;; (doseq [mm ['config 'before 'after 'success 'fail 'error 'run]] (ns-unmap *ns* mm))
;; (defn invoke-hook [method job & args]
;;   (apply method (into [(:name job) job] args)))

;; (invoke-hook run {:name "foo"} :foo)
