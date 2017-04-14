(ns dj-consumer.worker
  (:require
   [clojure.core.async :as async]
   [dj-consumer.database.core :as db]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.reserve-and-run :as rr]
   [dj-consumer.util :as u]
   [dj-consumer.job :as job]
   [dj-consumer.humanize :as humanize]

   [clj-time.core :as time]
   [clj-time.format :as time-format]
   [clj-time.coerce :as time-coerce]

   ;; String manipulation
   [cuerdas.core :as str]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]))

;;TODO: last-error set it properly on job

;; Catch exceptions in threads. Otherwise they disappear and we'd never know about them!!
 ;; https://stuartsierra.com/2015/05/27/clojure-uncaught-exceptions
(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (timbre/error ex "Uncaught exception on" (.getName thread)))))

(def defaults {:table :delayed-job

               ;;Worker behaviour
               :max-attempts 25
               :max-failed-reserve-count 10
               :delete-failed-jobs? false
               :on-reserve-fail :stop ;or :throw
               :exit-on-complete? false
               :poll-interval 5 ;sleep in seconds between batch jobs
               :poll-batch-size 100 ;how many jobs to process for every poll
               :reschedule-at (fn [some-time attempts]
                                (time/plus some-time (time/seconds (Math/pow attempts 4))))

               ;;Job selection:
               :min-priority nil
               :max-priority nil
               :queues nil ;nil is all queues, but nil is also a valid queue, eg [nil "q"]

               ;;Reporting
               :verbose? false
               :logger rr/default-logger
               :sql-log? false
               })

(defn stop-worker
  "Set worker status to :stopped"
  [{:keys [worker-status] :as env}]
  (reset! worker-status :stopped))

(defn start-worker
  "Start a async thread and loops till worker status changes to not running. In
  the loop first run a batch of jobs. Log the result if any jobs were done. If
  none were done, set worker status to :stopped if exit-on-complete? flag is
  set. Then, if worker status is :running sleep for poll-interval seconds and
  then recur loop. Otherwise clear locks and exit async thread"
  [{:keys [logger poll-interval exit-on-complete? worker-status] :as env}]
  (reset! worker-status :running)
  (async/go-loop []
    (let [{:keys [runtime]
           {:keys [success fail]}:result} (u/runtime (rr/run-job-batch env))
          total-runs (+ success fail)
          jobs-per-second (int (/ total-runs (/ runtime 1000.0)))]
      (when (pos? total-runs)
        (logger env :info (str total-runs " jobs processed at " jobs-per-second
                               " jobs per second. " fail " failed.")))
      (when (and (zero? total-runs) exit-on-complete?)
        (logger env :info "No more jobs available. Exiting")
        (stop-worker env)))
    (if (= @worker-status :running)
      (do
        (Thread/sleep poll-interval)
        (recur))
      ;;TODO Not sure is we need to clear these locks, superfluous??
      ;;Probably should be called before starting worker.
      (rr/clear-locks env))))

(defprotocol IWorker
  (start [this])
  (stop [this])
  (status [this])
  (env [this]))

(defrecord Worker [env]
  IWorker
  (start [this]
    (let [{:keys [logger log-atom]} env]
      (logger env :info "Starting")
      (let [{:keys [worker-status]} env]
        (if (not= @worker-status :running)
          (start-worker env)
          (logger env :info "Worker was already running")))))
  (stop [this]
    (let [{:keys [worker-status logger]} env]
      (logger env :info "Stopping")
      (if (= @worker-status :running)
        (stop-worker env)
        (logger env :info "Worker was already not running"))))
  (status [this] @(:worker-status env))
  (env [this] env))

(defn make-worker
  "Creates a worker that processes delayed jobs found in the db on a
  separate thread. Each worker is independant from other workers and
  can be run in parallel."
  [{:keys [worker-id verbose? db-conn db-config max-failed-reserve-count on-reserve-error] :as env}]
  {:pre [(some? worker-id)
         (or (nil? on-reserve-error) (contains? #{:stop :throw} on-reserve-error))
         (or (nil? max-failed-reserve-count) (number? max-failed-reserve-count))
         (some? (or db-conn db-config))]}
  (let [{:keys [db-conn db-config poll-interval worker-id] :as env} (merge defaults env)
        env (assoc env
                   :worker-id (str/strip-prefix (str worker-id) ":")
                   :poll-interval (* 1000 poll-interval)
                   :db-conn (or db-conn (db-conn/make-db-conn db-config)))]
    (when (:verbose? env)
      (info "Initializing dj-consumer with:")
      (pprint env))
    (->Worker (assoc env
                     :reserve-scope (db/make-reserve-scope env)
                     :failed-reserved-count (atom 0)
                     :max-run-time (* 3600 4) ;4 hours, hardcoded for every worker!!!!
                     :worker-status (atom nil)))))


;; (do
;;   (let [worker (make-worker{:worker-id :sample-worker
;;                             :sql-log? true
;;                             :verbose true
;;                             :db-config {:user "root"
;;                                         :password ""
;;                                         :url "//localhost:3306/"
;;                                         :db-name "chin_minimal"
;;                                         ;; :db-name "chinchilla_development"
;;                                         }

;;                             })]
;;     ;; (worker :start)
;;     (start worker)
;;     ;; (pprint (env worker))
;;     )

;;   ;; (pprint (db/sql :select-all-from {:table :delayed-job}))
;;   ;; (pprint (db/sql :get-cols-from-table {:table :delayed-job :cols [:id :handler]
;;   ;;                                       :where-clause
;;   ;;                                       (cl/conds->sqlvec :delayed-job "" nil [:id] [:id := 2988200])
;;   ;;                                       }))
;;   ;; (pprint handler-str)

;;   ;; (def handler-str (-> (db/sql :get-cols-from-table {:table :delayed-job :cols [:id :handler]
;;   ;;                                                    :where-clause
;;   ;;                                                    (cl/conds->sqlvec :delayed-job "" nil [:id] [:id := 2988200])
;;   ;;                                                    })
;;   ;;                      first
;;   ;;                      :handler))
;;   ;; (job/run:user/say-hello {:id 1} nil nil)
;;   ;; (job/after :user/say-hello {:id 1})
;;   ;; (job/run :invitation-expiration-reminder-job nil nil nil)
;;   ;; (pprint handler-str)
;;   ;; (pprint (parse-ruby-yaml handler-str))
;;   ;; (pprint (retrieve-jobs (merge @config {:now (now)})))
;;   )

;; (try
;;   (/ 3 0)
;;   (catch Exception e
;;     ;; (default-logger {:worker-id "foo-worker"} {:queue "baz-q" :id 1 :name "bar-job"} :info
;;     ;;                 (str (.toString e) "\nStacktrace:\n" (with-out-str (pprint (.getStackTrace e)))))
;;     ;; (throw e)
;;     (pprint (.getMessage e))
;;     ;; (pprint (.getCause e))
;;     (info (.toString e))
;;     ;; (info (with-out-str (pprint (.getStackTrace e))))
;;     ;; (pprint "blue")
;;     )
;;   (finally
;;     (pprint "finally")))


;; (do
;;   (let [c (chan)]
;;     (go
;;       (Thread/sleep 1000)
;;       (throw (ex-info "123" {}))
;;       (>! c :ok)
;;       )

;;     (pprint (<!! c)))
;;   )

;; (defn my-error []

;;   (Thread/sleep 4000)
;;   (throw (Exception. "from job"))
;;   (Thread/sleep 1000)
;;   (pprint "end")
;;   )
;; (def fut (future-call my-error))
;; (deref fut)

;; (thread (my-error))
;; (go (my-error))

;; (u/throwing-timeout my-error 2000)

;; (do
;;   (defn time-hogger []
;;     (loop [n 1]
;;       (/ n (+ 1 1.0))
;;       (if (and (not (Thread/interrupted)) (< n 2000000000))
;;         (recur (inc n)))
;;       )
;;     ;; (dotimes [n 10000000000]
;;     ;;   (/ n (+ n 1.0))
;;     ;;   )
;;     ;; (info (Thread/interrupted))
;;     ;; (info (Thread/interrupted))
;;     (if (Thread/interrupted)
;;       (pprint "interrupted!!")
;;       (pprint "Hello"))
;;     )
;;   (time (time-hogger)))

;; (u/throwing-timeout time-hogger 2000)


;; (defn throwing-timeout [env job f]
;;   (try
;;     (let [max-run-time 4000]
;;       (when (= (deref (future-call f) max-run-time :timeout
;;                       ) :timeout)
;;         (throw (ex-info (str "Job foo with name " (:name job) " has timed out.") {:timeout? true}))))
;;     (catch Exception e
;;       (let [{:keys [msg context]} (parse-ex-info e)]
;;         (cond
;;           (:timeout? context) (throw e)
;;           (.getCause e) (throw (.getCause e))
;;           :else (throw e)))

;;       )
;;     ))

;; (do
;;   ; http://stackoverflow.com/questions/11520394/why-do-cancelled-clojure-futures-continue-using-cpu/14540878#14540878


;;   ;; (throwing-timeout {} {:name "some-job"} (fn []
;;   ;;                                   (info "hello")
;;   ;;                                   (my-error)
;;   ;;                                   ))
;;   )


;;         (info (exception-str e))

;;         (info (exception-str (.getCause e)))
;;         (info (.toString e))
;;         (info (.toString (.getCause e)))
;;         (info (exception-str e))

;; (defn start-poll [env stop-ch]
;;   (go-loop []
;;     )
;;   )

;; (defn start-poll
;;   [env f time-in-ms]
;;   (let [stop (chan)]
;;     (go-loop []
;;       (alt!
;;         (timeout (:poll-interval env)) (do (<! (thread (f)))
;;                                  (recur))
;;         stop :stop))
;;     stop))


;; (defn start-worker [input-ch stop-ch]
;;   (go-loop []
;;     (let [[v ch] (alts! [input-ch stop-ch])]
;;       (if (identical? ch input-ch)
;;         (do
;;           (info "Value on input-ch is: " v)
;;           (recur))))))

;; (defn start [env stop-ch]
;;   (go-loop
;;       ))
;; (async/go-loop [i 0]
;;   (info "in go loop")
;;   (throw (Exception. "bla"))
;;   (if (< i 10)
;;     (recur (inc i))))
