(ns dj-consumer.sample-job              ;
  (:require
   [dj-consumer.job :as job]


   ;;Add following requires for debugging the job
   [dj-consumer
    [test-util :as tu]
    [reserve-and-run :as rr]
    [worker :as worker]]
   [clj-time.core :as time]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   ))


(defmethod job/run :invitation-expiration-reminder-job [job]
  (info "Doing job, sleeping 2000 ms")
  (Thread/sleep 1000)
  (info "Woke up!. Done the job " )
  )

(defmethod job/finally :invitation-expiration-reminder-job [job]
  (info "After some job " )
  )

(defmethod job/run :user/say-hello [job]
  (info "Running user job " )
  (info "Doing job, sleeping 2000 ms")
  (Thread/sleep 2000)
  (info "Woke up!. Done the job " )
  )

(defmethod job/finally :user/say-hello [job]
  (info "After user  job " )
  )

(comment
 (let [{:keys [env fixtures worker <!!-status log-atom]}
       (tu/setup-worker-test
        {:worker-config {;; :table :delayed-job

                         ;;Worker behaviour
                         ;; :max-attempts 25
                         ;; :delete-failed-jobs? false
                         ;; :on-reserve-fail :stop ;or :throw
                         ;; :max-failed-reserve-count 10
                         :exit-on-complete? true ;;
                         ;; :poll-interval 5 ;in seconds
                         ;; :reschedule-at (fn [some-time attempts]
                         ;;                  (time/plus some-time (time/seconds (Math/pow attempts 4))))

                         ;;Job selection:
                         ;; :min-priority nil
                         ;; :max-priority nil
                         ;; :queues nil ;nil is all queues, but nil is also a valid queue, eg [nil "q"]

                         ;;Reporting
                         ;; :job-batch-size 100 ;report success/fail every so many jobs
                         ;; :verbose? false
                         :logger rr/default-logger
                         :sql-log? false}


         :job-records [{:run-at tu/now :locked-by nil
                        :locked-at nil :priority 0 :attempts 0
                        :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})
                        :failed-at nil :queue nil}
                       {:run-at tu/now :locked-by nil
                        :locked-at nil :priority 0 :attempts 0
                        :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})
                        :failed-at nil :queue nil}]})
       worker2 (worker/make-worker {:exit-on-complete? true
                                    :db-config (:db-config env)
                                    :table (:table env)
                                    :worker-id :worker2})
       ]

   (with-redefs [dj-consumer.util/now (constantly tu/u-now)
                 ; dj-consumer.util/runtime  tu/mock-runtime
                 dj-consumer.util/exception-str (fn [e]
                                                  (str "Exception: " (.getMessage e)))
                 ]
     (worker/start worker)

     (worker/start worker2)

     (loop []
       (let [status (<!!-status)]
         (if-not (contains? #{:stopped :timeout :done :crashed} status)
           (recur)
           (timbre/info "Worker status: " status)))))

   (pprint (deref log-atom))
   (pprint (tu/job-table-data env))))

;;This does basically the same as above, but just runs once
(comment
  (with-redefs [dj-consumer.util/now (constantly tu/u-now)
                dj-consumer.util/runtime  tu/mock-runtime
                ;; dj-consumer.job/invoke-hook invoke-hook
                ]
    (let [job {:run-at tu/now :locked-by nil
               :locked-at nil :priority 0 :attempts 0 :failed-at nil
               :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})}
          {:keys [status fixtures log-atom job-table-data methods-called]}
          (tu/run-worker-once {:table :job-table } [job] (atom nil))]

      (info "Status:" status)
      (info "Log")
      (pprint log-atom)
      (info "Job table")
      (pprint job-table-data)
      )))
