(ns dj-consumer.worker
  (:require
   [clojure.core.async :as async]
   [dj-consumer.database.core :as db]
   [dj-consumer.database.connection :as db-conn]
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

(defn default-logger
  ([env level text]
   (default-logger env nil level text))
  ([{:keys [worker-id]} {:keys [id name queue] :as job} level text]
   (let [queue-str (if queue (str ", queue=" queue))
         job-str (if job (str "Job " name " (id=" id queue-str ") " ))
         text (str "[" worker-id "] " job-str text)]
     (log level text))))

;; (default-logger {:worker-id "foo-worker"} {:queue "baz-q" :id 1 :name "bar-job"} "hello" :info)

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
               :logger default-logger
               :sql-log? false
               })

(defn clear-locks
  "Clears all locks for worker"
  [{:keys [worker-id table] :as env}]
  (db/sql env :update-record (db/make-query-params env
                                                   {:table table
                                                    :updates {:locked-by nil :locked-at nil}
                                                    :where [:locked-by := worker-id]})))

(defn failed
  "Calls job fail hook, and then sets failed-at column on job record.
  Deletes record instead if job or env is configured accordingly"
  [{:keys [table logger delete-failed-jobs?] :as env} {:keys [id delete-if-failed? fail-reason] :as job}]
  (try
    (job/invoke-hook job/failed job)
    (catch Exception e
      (logger env job :error
              (str "Exception when running fail callback:\n" (u/exception-str e))))
    (finally
      (let [delete? (if (contains? job :delete-if-failed?)
                      delete-if-failed?
                      delete-failed-jobs?)
            query-params (db/make-query-params env {:table table
                                                    :where [:id := id]})]
        (if delete?
          (do
            (db/sql env :delete-record {:table table :id id})
            (logger env job :error (str "REMOVED permanently because of " fail-reason)))
          (do
            (db/sql env :update-record (db/make-query-params env
                                                             {:table table
                                                              :where [:id := id]
                                                              :updates {:failed-at (u/sql-time (u/now))}}))
            (logger env job :error (str "MARKED failed because of " fail-reason))))))))

(defn reschedule
  "Calculates new run-at using reschedule-at from env, unlocks and updates
  attempts and run-at of record"
  [{:keys [logger reschedule-at table] :as env} {:keys [id attempts] :as job}]
  (let [run-at (reschedule-at (u/now) attempts)]
    (db/sql env :update-record (db/make-query-params env
                                                     {:table table
                                                      :updates {:locked-by nil
                                                                :locked-at nil
                                                                :attempts attempts
                                                                :run-at (u/sql-time run-at)}
                                                      :where [:id := id]}))
    (logger env job :info (str "Rescheduled at " (u/time->str run-at)))))

(defn invoke-job
  "Tries to run actual job"
  [job]
  (try
    (job/invoke-hook job/run job)
    (catch Exception e
      (job/invoke-hook job/exception job)
      (throw e))
    (finally
      (job/invoke-hook job/finally job))))

(defn invoke-job-with-timeout
    "This runs the job's lifecycle methods in a thread and blocks till either job
  has completed or timeout expires, whichever comes first. If timeout occurs
  first, job is not actually stopped and will still run its course. However
  stop? key on job is an atom and will be set to true. Will throw if timeout
  occurs, or if job throws exception."
  [{:keys [max-run-time] :as job}]
  (let [timeout-channel (async/timeout max-run-time)
        job-channel (async/thread (try
                                    (invoke-job job)
                                    (catch Exception e
                                      e)))]
      (async/alt!!
        job-channel ([v _] (when (instance? Exception v)
                             (throw v)))
        timeout-channel (throw (ex-info (str "Job "(:name job) " has timed out.") {:timed-out? true})))))

(defn handle-run-exception
  "Logs job run exception. Reschedules job if attempts left and no
  fail is requested from job hooks, otherwise fails job. If job is
  timed out resets timed-out? atom of job to true "
  [{:keys [logger] :as env} {:keys [attempts] :as job} e]
  (let [max-attempts (or (:max-attempts job) (:max-attempts env))
        attempts (inc attempts)
        job (assoc job :attempts attempts)
        {{:keys [failed? timed-out?]} :context} (u/parse-ex-info e)
        too-many-attempts? (>= attempts max-attempts)]
    (logger env job :error
            (str "FAILED to run. "
                 (cond
                   failed? (str "Job requested fail")
                   too-many-attempts? (str "Failed " attempts " attempts."))
                 " Last exception:\n" (u/exception-str e)
                 ))
    (if (not (or failed? too-many-attempts?))
      (reschedule env job)
      (failed env (assoc job
                         :exception e
                         :fail-reason (if failed?
                                        "job requested to be failed"
                                        (str attempts " consecutive failures"))
                         )))
    (when timed-out?
      ;;Communicate to job thread that job is timed out.
      (reset! (:timed-out? job) true))))

(defn run
  "Times and runs a job. A failing job should throw an exception. A
  successful job gets deleted. A failed job is potentially
  rescheduled. Returns either :success or :fail"
  [{:keys [logger table] :as env} {:keys [id attempts] :as job}]
  (logger env job :info "RUNNING")
  (try
    (let [{:keys [runtime]} (u/runtime (invoke-job-with-timeout job))]
      (db/sql env :delete-record {:id id :table table})
      (logger env job :info (str "COMPLETED after " (humanize/duration runtime)))
      :success)
    (catch Exception e
      (handle-run-exception env job e)
      :fail)))

(defn reserve-job
  "Looks for and locks a suitable job in one transaction. Returns that
  job if found or otherwise nil. Handler column of job record is
  assumed to be yaml and parsed into a map with a job and data key"
  [{:keys [table worker-id max-run-time] :as env}]
  (let [now (u/now)
        lock-job-scope (db/make-lock-job-scope env now)
        ;;Lock a job record
        locked-job-count (db/sql env :update-record lock-job-scope)]
    (info "locked-job-count" locked-job-count)
    (if (pos? locked-job-count)
      (let [query-params (db/make-query-params env
                                               {:table table
                                                :where [:and [[:locked-at := (u/to-sql-time-string now)]
                                                              [:locked-by := worker-id]
                                                              [:failed-at :is :null]]]})
            ;;Retrieve locked record
            job (first (db/sql env :get-cols-from-table query-params))
            job-config (job/invoke-hook job/config job)
            job (merge job (try (u/parse-ruby-yaml (:handler job))
                                (catch Exception e
                                  (throw (ex-info "Exception thrown parsing job yaml"
                                                  {:e e
                                                   :job job
                                                   :yaml-exception? true})))))]
        (merge job job-config {:timed-out? (atom false)
                               :max-run-time (min max-run-time (or (:max-run-time job-config)
                                                                   max-run-time))})))))
(declare stop-worker)

(defn reserve-and-run-one-job
  "If a job can be reserved returns the result of the run (:success
  or :fail), otherwise returns nil. If configured will throw on too
  many reserve errors."
  [{:keys [logger on-reserve-fail max-failed-reserve-count failed-reserved-count] :as env}]
  (try
    (if-let [job (reserve-job env)]
      (run env job)) ;never throws, just returns :success or :fail
    (catch Exception e
      (let [{{:keys [job yaml-exception?]} :context} (u/parse-ex-info e)]
        (if yaml-exception?
          (failed env (assoc job
                             :exception e
                             :fail-reason "yaml parse error"
                             )) ;fail job immediately, yaml is no good.
          (do ;Panic! Reserving job went wrong!!!
             (logger env :error (str "Error while trying to reserve a job: \n" (u/exception-str e)))
            (let [fail-count (swap! failed-reserved-count inc)]
              (when (> fail-count max-failed-reserve-count)
                (condp = on-reserve-fail
                  :stop (stop-worker env)
                  :throw (throw (ex-info "Failed to reserve jobs"
                                         {:last-exception e
                                          :failed-reserved-count @failed-reserved-count}))))))))
      :fail)))

(defn run-job-batch
  "Run a number of jobs in one batch, consecutively. Return map of
  success and fail count. We're running in a thread, so we check
  worker status and stop processing the batch if worker status in
  not :running"
  [{:keys [exit-on-complete? poll-batch-size worker-status] :as env}]
  (loop [result-count {:success 0 :fail 0}
         counter 0]
    (if-let [success-or-fail (and (= @worker-status :running)
                                  (< counter poll-batch-size)
                                  (reserve-and-run-one-job env))]
      (recur (update result-count success-or-fail inc)
             (inc counter))
      result-count)))

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
  (pprint (reserve-job env))
  (async/go-loop []
    (let [{:keys [runtime]
           {:keys [success fail]}:result} (u/runtime (run-job-batch env))
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
      (clear-locks env))))

(defprotocol IWorker
  (start [this])
  (stop [this])
  (status [this])
  (env [this]))

(defrecord Worker [env]
  IWorker
  (start [this]
    (let [{:keys [logger]} env]
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
;;     ;; (start worker)
;;     (pprint (env worker))
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
