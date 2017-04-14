(ns dj-consumer.job
  (:require
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]))

;; The various lifecycle multimethods for a job are dispatched by name of the
;; job. Every job has a :timed-out? key. This is an atom that's set to true if
;; job times out. The various methods of job get called, regardless of the value
;; of timed-out?. Take appropriate action in each method.
(defn dispatch [k job] k)

;;Called when job is reserved.
(defmulti config
  "Return a map with overrides and extra options for the job Extra
  options are :max-run-time :max-attempts, destroy-failed-job"
  dispatch)

;;Start job lifecycle (in order)
(defmulti run
  "Run job, considered failed if it throws an exception, otherwise is
  considered success" dispatch)

(defmulti exception
  "Called if run throws an exception, exception will be on job under
  exception key. " dispatch)

(defmulti finally
  "Always called, exception thrown or not" dispatch)
;;End job lifecycle

(defmulti failed
  "Called after trying to run job max-attempts times and job's still
  throwing an exception from either run, exception or finally hooks,
  or timing out. This hook is called immediately if the exception
  thrown is an ex-info with context set to {:failed? true}" dispatch)

(defmethod config :default [_ job])
(defmethod run :default [k job] (throw (ex-info (str "Implementation is missing for job " k)
                                                {:failed? true})))
(defmethod exception :default [_ job])
(defmethod finally :default [_ job])
(defmethod failed :default [_ job])

(defn invoke-hook
  "Calls hook on job with job and any extra args"
  [method job & args]
  (apply method (into [(:name job) job] args)))


;; (doseq [mm ['config 'after 'fail 'error 'run]] (ns-unmap *ns* mm))
;; (defn invoke-hook [method job & args]
;;   (apply method (into [(:name job) job] args)))

;; (invoke-hook run {:name "foo"} :foo)
