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


(defmethod job/perform :invitation-expiration-reminder-job [_ job _ _]
  (info "Doing job, sleeping 1000 ms")
  (Thread/sleep 1000)
  (info "Woke up!. Done the job " )
  )

(defmethod job/after :invitation-expiration-reminder-job [_ job _ _]
  (info "After some job " )
  )

(defmethod job/perform :user/say-hello [_ job _ _]
  (info "Performing user job " )
  )

(defmethod job/after :user/say-hello [_ job]
  (info "After user  job " )
  )
