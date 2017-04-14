(ns dj-consumer.worker-test
  (:require
   [dj-consumer.database.core :as db]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.util :as u]
   [dj-consumer.job :as job]
   [dj-consumer.test-util :as tu]

   [clojure.test :as t :refer [deftest is use-fixtures testing]]
   [clojure.core.async :as async  :refer (<! <!! >! >!! put! take! alts! alts!! chan go go-loop close!)]

   [clj-time.core :as time]
   [clj-time.coerce :as time-coerce]
   [clj-time.local :as time-local]
   [clojure.set :as set]

   [cuerdas.core :as str]

   [dj-consumer.worker :as worker]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   ))


(def methods-called (atom {}))
(def log-atom (atom []))

(defmethod job/run :job1 [k job]
  (swap! methods-called assoc-in [k :run] job)
  )

(def u-now (u/now))
(def now (u/sql-time u-now))
(def a-minute-ago (time/minus now (time/minutes 1)))
(def two-minutes-ago (time/minus now (time/minutes 2)))
(def three-minutes-ago (time/minus now (time/minutes 3)))
(def five-hours-ago (time/minus now (time/hours 5)))

;; (deftest worker
;;   (let [{:keys [env worker fixtures]}
;;         (tu/prepare-for-test-merge-worker-config tu/defaults
;;                                                  {:logger tu/test-logger
;;                                                   :log-atom log-atom
;;                                                   :exit-on-complete? true

;;                                                   ;; :max-attempts 25
;;                                                   ;; :max-failed-reserve-count 10
;;                                                   ;; :delete-failed-jobs? false
;;                                                   ;; :on-reserve-fail :stop ;or :throw
;;                                                   ;; :poll-interval 5 ;sleep in seconds between batch jobs
;;                                                   ;; :poll-batch-size 100 ;how many jobs to process for every poll
;;                                                   ;; :reschedule-at (fn [some-time attempts]
;;                                                   ;;                  (time/plus some-time (time/seconds (Math/pow attempts 4))))

;;                                                   ;; ;;Job selection:
;;                                                   ;; :min-priority nil
;;                                                   ;; :max-priority nil
;;                                                   ;; :queues nil ;nil is all queues, but nil is also a valid queue, eg [nil "q"]
;;                                                   }
;;                                               [{:priority nil
;;                                                 :attempts 0
;;                                                 :handler nil
;;                                                 :last-error nil
;;                                                 :run-at now
;;                                                 :locked-at nil
;;                                                 :failed-at nil
;;                                                 :locked-by nil
;;                                                 :queue nil}

;;                                                ])
;;         ]
;;     (reset! log-atom [])
;;     ;; (tn/start worker)
;;     (is (= (tu/job-table-data env) :??))
;;     )
;;   )

;; (defn stop-worker
;;   "Set worker status to :stopped"
;;   [{:keys [worker-status] :as env}]
;;   (reset! worker-status :stopped))
(with-redefs [dj-consumer.util/exception-str (fn [e]
                                               (str "Exception: " (.getMessage e)))
              ;; dj-consumer.worker/stop-worker (fn [{:keys [log-ch] :as env}]
              ;;                                  (put! log-ch :stop)
              ;;                                  (info "in test stop worker")
              ;;                                  (stop-worker env))
              ]


  (do
    (def log-ch (chan))
    (def log-atom (atom []))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults
                                                   {:logger tu/test-logger
                                                    :log-atom log-atom
                                                    :exit-on-complete? true
                                                    :log-ch log-ch

                                                    ;; :max-attempts 25
                                                    ;; :max-failed-reserve-count 10
                                                    ;; :delete-failed-jobs? false
                                                    ;; :on-reserve-fail :stop ;or :throw
                                                    :poll-interval 1 ;sleep in seconds between batch jobs
                                                    ;; :poll-batch-size 100 ;how many jobs to process for every poll
                                                    ;; :reschedule-at (fn [some-time attempts]
                                                    ;;                  (time/plus some-time (time/seconds (Math/pow attempts 4))))

                                                    ;; ;;Job selection:
                                                    ;; :min-priority nil
                                                    ;; :max-priority nil
                                                    ;; :queues nil ;nil is all queues, but nil is also a valid queue, eg [nil "q"]
                                                    }
                                                   [{:priority nil
                                                     :attempts 0
                                                     :handler nil
                                                     :last-error nil
                                                     :run-at now
                                                     :locked-at nil
                                                     :failed-at nil
                                                     :locked-by nil
                                                     :queue nil}])]
      (def tworker worker)
      (def tfixtures fixtures)
      (def tenv env)
      ))


  (do

    (let [{:keys [worker-status]} tenv]
      (add-watch worker-status :status (fn [_ _ _ status]
                                         (if (= status :stopped)
                                           (put! log-ch :stop))
                                         )))
    (reset! log-atom [])
    (def stop-worker worker/stop-worker)
    (def timeout-channel  (async/timeout 10000))


    (worker/start tworker)

    (loop [counter 0]
      (let [{:keys [job text level] :as result}
            (async/alt!!
              log-ch ([v _] v)
              timeout-channel :timeout)]
        (if-not (or (= result :stop)
                    (= result :timeout)
                    ;; (str/contains? text "Exiting")
                    (> counter 10))
          (recur (inc counter))))))

  (pprint (deref log-atom))
  (pprint (tu/job-table-data tenv))
  )






;; (deftest run-job-batch

;;   )

;; (deftest stop-worker)


;; (deftest make-worker)

;; (def t1
;;   (time/date-time 10 1 1))
;; (def t2
;;   (time/date-time 10 1 1))
;; (= t1 t2)

;; (time-coerce/to-sql-time)
;; (time/default-time-zone)
;; (pprint (time/time-zone-for-id "Europe/Amsterdam"))
;; ;; => #object[org.joda.time.DateTime 0x41c55407 "2017-04-12T09:50:52.477Z"]
;; (time/now)
;; ;; => #object[org.joda.time.DateTime 0x430fbf33 "2017-04-12T09:51:25.716Z"]
;; (time/date-time 1986 10 14 9 0 0 456)
;; ;; => #object[org.joda.time.DateTime 0x84d4838 "1986-10-14T09:00:00.456Z"]
;; (time/default-time-zone)
;; tz
;; ;; => #object[org.joda.time.LocalTime 0x49b184be "11:52:50.458"]
;; (def now-in-ms (time-coerce/to-long (time/now)))

;; (do
;;   (defn now
;;     "Using local computer time."
;;     []
;;     (let [now-in-ms (time-coerce/to-long (time/now))
;;           offset (/ (.getOffset (time/default-time-zone) now-in-ms) 1000)
;;           hours (int (/ offset 3600))
;;           minutes (int (/ (rem offset 3600) 60))]
;;       hours
;;       minutes
;;       (time/from-time-zone (time/now) (time/time-zone-for-offset hours minutes))))
;;   (now))


;; (def now-in-ms (time-coerce/to-long ))
;; (time-coerce/to-sql-time (time/now))
;; ;; => #inst "2017-04-12T10:12:11.048000000-00:00"
;; (time/now)
;; ;; => #object[org.joda.time.DateTime 0x37125bd4 "2017-04-12T10:12:14.973Z"]
;; (def a (let [a 1
;;              c d]

;;          ))
;; (def c (chan))
;; (put! c :foo)
;; (put! c :stop)
;; (take! c (fn [v] (info v) v))
;; (go-loop []
;;   (info "Waiting for value")
;;   (let [v (<! c)]
;;     (info "Received value:" v)
;;     (if (not= v :stop)
;;       (recur)
;;       (info "stopped")))
;;   )


;; (def s "bla foo Exiting")
;; (str/contains? s "Exiting")
