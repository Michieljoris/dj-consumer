(ns dj-consumer.worker
  (:require
   [clojure.core.async :as async]
   [dj-consumer.database :as db]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.util :as u]
   [dj-consumer.job :as j]
   [dj-consumer.humanize :as humanize]

   [clj-time.core :as time]

   ;; String manipulation
   [cuerdas.core :as str]

   [dj-consumer.sample-job]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]))

;; Catch exceptions in threads.. 
 ;; https://stuartsierra.com/2015/05/27/clojure-uncaught-exceptions
(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (timbre/error ex "Uncaught exception on" (.getName thread)))))

(defn default-logger
  ([env level text]
   (default-logger env nil text level))
  ([{:keys [worker-id]} {:keys [id name queue] :as job} level text]
   (let [job-str (if job (str "Job " name " (id=" id ") " ))
         queue-str (if queue (str "(queue=" queue ") "))
         text (str "[" worker-id "] " job-str queue-str text)]
     (log level text))))

;; (default-logger {:worker-id "foo-worker"} {:queue "baz-q" :id 1 :name "bar-job"} "hello" :info)

(def defaults {:max-attempts 25
               :max-run-time (* 3600 4) ;4 hours
               :reschedule-at (fn [t-in-s attempts] (int (+ t-in-s (Math/pow attempts 4))))
               :listen? false
               :delete-failed-jobs? false
               :poll-interval 5 ;in seconds
               :logger default-logger
               :sql-log? false
               :table :delayed-job
               :min-priority 0
               :max-priority 10
               :max-failed-reserve-count 10
               :throw-on-reserve-fail false
               ;;Queues to process: nil processes all, [nil] processes nil
               ;;queues, ["foo" nil] processes nil and "foo" queues
               :queues nil})

(defn now
  "Using local computer time."
  []
  (time/now))

;;TODO: make sure to run on exit/stopping/halting of job runner!!!!
(defn clear-locks
  "Clears all locks for worker"
  [{:keys [worker-id table] :as env}]
  (db/sql env :update-record (db/make-query-params {:table table
                                                    :updates {:locked-by nil :locked-at nil}
                                                    :where [:locked-by worker-id]})))

(defn exception-str [e]
  (str (.toString e) "\nStacktrace:\n" (with-out-str (pprint (.getStackTrace e)))))

(defn invoke-hook
  "Calls hook on job with job and any extra args"
  [method job & args]
  (apply method (into [(:name job) job] args)))

(defn failed
  "Calls job fail hook, and then sets failed-at column on job record.
  Deletes record instead if job or env is configured accordingly"
  [{:keys [logger delete-failed-jobs?] :as env} {:keys [attempts] :as job}]
  (try
    (invoke-hook j/fail job)
    (catch Exception e
      (logger env job :error 
              (str "Error when running fail callback:\n" (exception-str e))))
    (finally
      (let [delete? (or (:delete-if-failed? job)
                        delete-failed-jobs?)
            query-params {:table (:table env)
                          :id (:id job)}]
        (if delete?
          (do
            (db/sql env :delete-record query-params)
            (logger env job :error (str "REMOVED permanently because of " attempts
                                        "consecutive failures")))
          (do
            (db/sql env :update-record (merge query-params
                                              {:updates {:failed-at (u/to-sql-time-string (now))}}))
            (logger env job :error (str "MARKED failed because of " attempts
                                        "consecutive failures"))))))))

(defn reschedule
  "If there are less attempts made then max-attempts reschedules job.
  Otherwise job will be processed as failed"
  [{:keys [logger reschedule-at table] :as env} {:keys [id attempts] :as job}]
  (let [run-at (reschedule-at (now) attempts)]
    (db/sql env :update-record (db/make-query-params {:table table
                                                      :updates {:locked-by nil
                                                                :locked-at nil
                                                                :attempts attempts 
                                                                :run-at run-at}
                                                      :where [:id := id]}))
    (logger env job :info "Rescheduled at " run-at)))

(defn invoke-job
  "Tries to run actual job, by invoking its various job hooks."
  [job]
  (try
    (invoke-hook j/before job)
    (invoke-hook j/run job)
    (invoke-hook j/success job)
    (catch Exception e
      (invoke-hook j/error job)
      (throw e))
    (finally
      (invoke-hook j/after job))))

(defn timeout
    "This blocks till either f has completed or timeout expires,
  whichever comes first. If timeout occurs first, thread is not
  actually stopped and will still run its course. However it's
  possible to deref stop? prop of job in f and end early. Will throw
  if timeout occers, or if f throws exception Otherwise returns
  evaluation of f"
  [f {:keys [max-run-time] :as job}]
  (let [timeout-channel (async/timeout max-run-time)
        job-channel (async/thread (try
                              (f job)
                              (catch Exception e
                                e)))]
      (async/alt!! 
        job-channel ([v _] (if (instance? Exception v)
                             (throw v)
                             v))
        timeout-channel (do
                          (reset! (:stop? job) true)
                          (throw (ex-info (str "Job "(:name job) " has timed out.") {:timeout? true}))))))

(defn run
  "Times and runs a job. A failing job should throw an exception. A
  successful job gets deleted. A failed job is (attempted to be)
  rescheduled. Returns either :success or :fail"
  [{:keys [logger table] :as env} {:keys [id attempts] :as job}]
  (logger env job :info "RUNNING")
  (try
    (let [runtime (u/time-in-ms (timeout invoke-job job))]
      (db/sql env :delete-record {:id id :table table})
      (logger env job :info "COMPLETED after " (humanize/duration runtime))
      :success)
    (catch Exception e
      (logger env job :error
              (str "FAILED to run. " "Failed " attempts " prior attempts. Error this time:\n")
              (exception-str e))
      (let [max-attempts (or (:max-attempts job) (:max-attempts env))
            attempts (inc attempts)
            job (assoc job :attempts attempts)]
        (if (< attempts max-attempts)
          (reschedule env job)
          (failed env job)))
      :fail)))

(defn reserve-job
  "Looks for and locks a suitable job in one transaction. Returns that
  job if found or otherwise nil. Handler column of job record is
  assumed to be yaml and parsed into a map with a job and data key"
  [{:keys [table worker-id max-run-time] :as env}]
  (let [now (now)
        lock-job-scope (db/make-lock-job-scope env now)
        ;;Lock a job record
        locked-job-count (db/sql env :update-record lock-job-scope)]
    (if (pos? locked-job-count)
      (let [now (u/to-sql-time-string now)
            query-params (db/make-query-params {:table table
                                                :where [:and [[:locked-at := now]
                                                              [:locked-by := worker-id]
                                                              [:failed-at :is :null]]]})
            ;;Retrieve locked record 
            job (first (db/sql env :get-cols-from-table query-params))
            job (merge job (u/parse-ruby-yaml (:handler job)))
            job-config (invoke-hook j/config job)]
        (merge job job-config {:stop? (atom false)
                               :max-run-time (min max-run-time (or (:max-run-time job-config)
                                                                   max-run-time))})))))

(def failed-reserved-count (atom 0))

(defn reserve-and-run-one-job
  "If a job could be reserved returns the result of the run (:success
  or :fail), otherwise returns nil. If configured will throw on too
  many reserve errors."
  [{:keys [logger throw-on-reserve-fail max-failed-reserve-count] :as env}]
  (try
    (if-let [job (reserve-job env)]
      (run env job))
    ;;TODO catch exception of misconfigured job and call failed
    (catch Exception e
      (logger env :error (str "Error while reserving job: " (.toString e)))
      (let [fail-count (swap! failed-reserved-count inc)]
        (if (and throw-on-reserve-fail (> fail-count max-failed-reserve-count))
          (throw (ex-info "Failed to reserve jobs" {:failed-reserved-count @failed-reserved-count})))))))

;; (defn work-off [n]
;;  (loop [c]) 
;;   )

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


(defn start-worker [input-ch stop-ch]
  (go-loop []
    (let [[v ch] (alts! [input-ch stop-ch])]
      (if (identical? ch input-ch)
        (do
          (info "Value on input-ch is: " v)
          (recur))))))

;; (defn start [env stop-ch]
;;   (go-loop
;;       ))

(defprotocol IWorker
  (start [this])
  (stop [this]))

(defrecord Worker [env stop-ch]
  IWorker
  (start [this]
    (info "Starting worker")
    (pprint (dissoc (reserve-job env) :handler))
    )
  (stop [this]))

(defn make-worker [env]
  {:pre [(some? (:worker-id env))
         (some? (or (:db-conn env) (:db-config env)))]}
  (let [{:keys [db-conn db-config poll-interval worker-id] :as env} (merge defaults env)
        env (assoc env
                   :worker-id (str/strip-prefix (str worker-id) ":")
                   :poll-interval (* 1000 poll-interval)
                   :db-conn (or db-conn (db-conn/make-db-conn db-config)))]
    (when (:verbose env)
      (info "Initializing dj-consumer with:")
      (pprint env))
    (->Worker (assoc env :reserve-scope (db/make-reserve-scope env)) 
              (chan))))

;; (def input-ch (chan))
;; (def stop-ch (chan))
;; (close! stop-ch)
;; (close! input-ch)

;; (start-worker input-ch stop-ch)
;; (>!! input-ch "hello")
;; (a/put! input-ch "hello")
;; (a/put!  stop-ch "hello")


;; (def job-queue (atom []))

;timeout, async, lock table, all in one sql transaction?,
;; (defn try-job [{:keys [locked-at locked-by failed-at] :as job-record}]
;;   (try
;;     :foo
;;     (catch Exception e :foo))
;;   )

;; (defn process-jobs [jobs]

;;   )



;; AILS_ENV=production script/delayed_job start
;; RAILS_ENV=production script/delayed_job stop

;; # Runs two workers in separate processes.
;; RAILS_ENV=production script/delayed_job -n 2 start
;; RAILS_ENV=production script/delayed_job stop

;; # Set the --queue or --queues option to work from a particular queue.
;; RAILS_ENV=production script/delayed_job --queue=tracking start
;; RAILS_ENV=production script/delayed_job --queues=mailers,tasks start

;; # Use the --pool option to specify a worker pool. You can use this option multiple times to start different numbers of workers for different queues.
;; # The following command will start 1 worker for the tracking queue,
;; # 2 workers for the mailers and tasks queues, and 2 workers for any jobs:
;; RAILS_ENV=production script/delayed_job --pool=tracking --pool=mailers,tasks:2 --pool=*:2 start

;; # Runs all available jobs and then exits
;; RAILS_ENV=production script/delayed_job start --exit-on-complete
;; # or to run in the foreground
;; RAILS_ENV=production script/delayed_job run --exit-on-complete

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
;;     (start worker))

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

        
        ;; (info (exception-str e))
        
        ;; (info (exception-str (.getCause e))) 
        ;; (info (.toString e)) 
        ;; (info (.toString (.getCause e))) 
        ;; (info (exception-str e))
(do
  (def p (promise))
  (deliver p (do (pprint "doing promise") (Thread/sleep 1000) (pprint "done") :done))
  (Thread/sleep 2000)
  (pprint "deref promise")
  (pprint @p))
;;Prints:
;; "doing promise"
;; <one second pause>
;; "done"
;; <two seconds pause>
;; "deref promise"
;; :done

(do
  (def d (delay (do (pprint "doing delay") (Thread/sleep 1000) (pprint "done") :done)))
  (Thread/sleep 2000)
  (pprint "force delay")
  (pprint (force d)))
;;Prints:
;; <two seconds pause>
;; "force delay"
;; "doing delay"
;; <one second pause>
;; "done"
;; :done

(do
  (def f (future (do (pprint "doing future") (Thread/sleep 1000) (pprint "done") :done)))
  (Thread/sleep 2000)
  (pprint "deref future")
  (pprint (deref f)))
;;Prints:
;; "doing future"
;; <one second pause>
;; "done"
;; <one second pause>
;; "deref future"
;; :done


















































