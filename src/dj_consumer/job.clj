(ns dj-consumer.job
  (:require

   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]))

;; (ns-unmap *ns* 'perform)

(defmulti before "Called before first attempt at job" (fn [k job] k))
(defmulti perform "Perform job, fails if it throws an exception, otherwise is considered success"
  (fn [k job c] k))
(defmulti after "Called after successful job or after doing max-attempts and still failing" (fn [k job] k))
(defmulti success "Called after a job is successful" (fn [k job] k))
(defmulti fail "Called after trying to perform job max-attempts and job's still failing"(fn [k job] k))
(defmulti error "Called if perform throws an exception"(fn [k job exception] k))
(defmulti config "Called to get job specific configutation. Options are :max-run-time :max-attempts, destroy-failed-job" (fn [k job] k))

(defmethod before :default [_ _])
(defmethod perform :default [k _ _] (timbre/error "Implementation is missing for job " k))
(defmethod after :default [_ _])
(defmethod success :default [_ _])
(defmethod fail :default [_ _])
(defmethod error :default [_ _])
(defmethod config :default [_ _] {:async false})
