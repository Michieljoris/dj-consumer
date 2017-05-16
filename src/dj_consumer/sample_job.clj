(ns dj-consumer.sample-job
  (:require
   [dj-consumer.job :as job]



   ;;Add following requires for debugging the job
   [dj-consumer
    [test-util :as tu]
    [reserve-and-run :as rr]
    [worker :as worker]]
   [digicheck.common.util :as u]
   [clj-time.core :as time]
   [clojure.core.async :as async]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   ))

(defn do-work
  "About 100ms"
  []
  (dotimes [n 10000000]
    (* n n)))

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
  (info "Doing job, sleeping 10s")
  ;; (do-work)
  (Thread/sleep 10000)
  (if @(:timed-out? job)
    (info "Ah bummer, timed out " )
    (info "Woke up!. Done the job " ))
  )
(defmethod job/config :user/say-hello [job] {:max-run-time 100 :max-attempts 1})

(defmethod job/finally :user/say-hello [job]
  (info "After user  job " )
  )

(defn make-extra-worker [env worker-id]
  (let [worker-setup
        (tu/setup-worker-test
         {:worker-config {:exit-on-complete? true
                          :logger rr/default-logger
                          :sql-log? false
                          :db-config (:db-config env)
                          :table (:table env)
                          :worker-id worker-id
                          }
          :job-records nil})
        worker (:worker worker-setup)]
    (worker/start worker)
   (:<!!-status worker-setup)))

;; Start 6 workers for 6 100ms jobs, takes about 176ms in total.
;; (do
;;  (let [{:keys [env fixtures worker <!!-status log-atom]}
;;        (tu/setup-worker-test
;;         {:worker-config {;; :table :delayed-job

;;                          ;;Worker behaviour
;;                          ;; :max-attempts 25
;;                          ;; :delete-failed-jobs? false
;;                          ;; :on-reserve-fail :stop ;or :throw
;;                          ;; :max-failed-reserve-count 10
;;                          :exit-on-complete? false ;;
;;                          ;; :poll-interval 5 ;in seconds
;;                          ;; :reschedule-at (fn [some-time attempts]
;;                          ;;                  (time/plus some-time (time/seconds (Math/pow attempts 4))))

;;                          ;;Job selection:
;;                          ;; :min-priority nil
;;                          ;; :max-priority nil
;;                          ;; :queues nil ;nil is all queues, but nil is also a valid queue, eg [nil "q"]

;;                          ;;Reporting
;;                          ;; :job-batch-size 100 ;report success/fail every so many jobs
;;                          ;; :verbose? false
;;                          :logger rr/default-logger
;;                          :sql-log? false}


;;          :job-records [{:run-at (u/sql-time (u/now)) :locked-by nil
;;                         :locked-at nil :priority 0 :attempts 0
;;                         :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})
;;                         :failed-at nil :queue nil}
;;                        {:run-at (u/sql-time (time/plus (u/now) (time/seconds 0)))
;;                         :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})
;;                         }
;;                        ;; {:run-at (u/sql-time (u/now))
;;                        ;;  :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})
;;                        ;;  }
;;                        ;; {:run-at (u/sql-time (u/now))
;;                        ;;  :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})
;;                        ;;  }
;;                        ;; {:run-at (u/sql-time (u/now))
;;                        ;;  :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})
;;                        ;;  }
;;                        ;; {:run-at (u/sql-time (u/now))
;;                        ;;  :handler (tu/make-handler-yaml {:job-name :user/say-hello :payload {:foo 123}})
;;                        ;;  }
;;                        ]})
;;        ;; <!!-status2 (make-extra-worker env :worker2)
;;        ;; <!!-status3 (make-extra-worker env :worker3)
;;        ;; <!!-status4 (make-extra-worker env :worker4)
;;        ;; <!!-status5 (make-extra-worker env :worker5)
;;        ;; <!!-status6 (make-extra-worker env :worker6)
;;        ;;  worker2-setup
;;        ;;  (tu/setup-worker-test
;;        ;;   {:worker-config {:exit-on-complete? true
;;        ;;                    :logger rr/default-logger
;;        ;;                    :sql-log? false
;;        ;;                    :db-config (:db-config env)
;;        ;;                    :table (:table env)
;;        ;;                    :worker-id :worker2
;;        ;;                    }
;;        ;;    :job-records nil})
;;        ;; worker2 (:worker worker2-setup)
;;        ;; <!!-status2 (:<!!-status worker2-setup)

;;        ;;  worker3-setup
;;        ;;  (tu/setup-worker-test
;;        ;;   {:worker-config {:exit-on-complete? true
;;        ;;                    :logger rr/default-logger
;;        ;;                    :sql-log? false
;;        ;;                    :db-config (:db-config env)
;;        ;;                    :table (:table env)
;;        ;;                    :worker-id :worker3
;;        ;;                    }
;;        ;;    :job-records nil})
;;        ;; worker3 (:worker worker3-setup)
;;        ;; <!!-status3 (:<!!-status worker3-setup)

;;        ]

;;    (with-redefs [ dj-consumer.util/now (constantly tu/u-now)
;;                  ; dj-consumer.util/runtime  tu/mock-runtime
;;                  dj-consumer.util/exception-str (fn [e]
;;                                                   (str "Exception: " (.getMessage e)))]
;;      (worker/start worker)
;;      (def worker worker)
;;      ;; (worker/start worker2)
;;      ;; (worker/start worker3)

;;      ;; (time
;;      ;;  (do
;;      ;;    (let [status (<!!-status)]
;;      ;;      (timbre/info "Worker 1 status: " status))

;;      ;;    (let [status (<!!-status2)]
;;      ;;      (timbre/info "Worker 2 status: " status))

;;      ;;    (let [status (<!!-status3)]
;;      ;;      (timbre/info "Worker 3 status: " status))
;;      ;;    (let [status (<!!-status4)]
;;      ;;      (timbre/info "Worker 4 status: " status))
;;      ;;    (let [status (<!!-status5)]
;;      ;;      (timbre/info "Worker 5 status: " status))

;;      ;;    (let [status (<!!-status6)]
;;      ;;      (timbre/info "Worker 6 status: " status))
;;      ;;    ))


;;      ;;  (let [statuses (async/<!! (async/map vector [<!!-status <!!-status2]))]
;;      ;;    (info "Statuses:" statuses))
;;      ;; (time
;;      ;;  (do
;;      ;;    (loop []
;;      ;;      (let [status (<!!-status)]
;;      ;;        (if-not (contains? #{:stopped :timeout :done :crashed} status)
;;      ;;          (recur)
;;      ;;          (timbre/info "Worker status: " status))))

;;      ;;    ;; (loop []
;;      ;;    ;;   (let [status (<!!-status2)]
;;      ;;    ;;     (if-not (contains? #{:stopped :timeout :done :crashed} status)
;;      ;;    ;;       (recur)
;;      ;;    ;;       (timbre/info "Worker status: " status))))
;;      ;;    ))
;;      )

;;    (pprint (deref log-atom))
;;    (pprint (tu/job-table-data env))))

;; This does basically the same as above, but just runs once
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

(comment
  (worker/status worker)
  (worker/stop worker)
  (worker/start worker)
  (pprint (worker/env worker)))
