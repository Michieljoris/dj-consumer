(ns dj-consumer.sample-job
  (:require
   [dj-consumer.job :as job]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   ))


(defmethod job/run :invitation-expiration-reminder-job [_ job _ c]
  (info "Doing job, sleeping 1000 ms")
  (Thread/sleep 1000)
  (info "Woke up!. Done the job " )
  ;; (put! c :done)
  )

(defmethod job/finally :invitation-expiration-reminder-job [_ job _ _]
  (info "After some job " )
  )

(defmethod job/run :user/say-hello [_ job _ _]
  (info "Running user job " )
  )

(defmethod job/finally :user/say-hello [_ job]
  (info "After user  job " )
  )
