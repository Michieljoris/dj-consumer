(ns dj-consumer.reserve-and-run
  (:require [clojure.core.async :as async]
            [dj-consumer
             [humanize :as humanize]
             [job :as job]
             [util :as u]]
            [dj-consumer.database.core :as db]
            [taoensso.timbre :as timbre :refer [log]]))

(defn default-logger
  ([env level text]
   (default-logger env nil level text))
  ([{:keys [worker-id]} {:keys [id name queue] :as job} level text]
   (let [queue-str (if queue (str ", queue=" queue))
         job-str (if job (str "Job " name " (id=" id queue-str ") " ))
         text (str "[" worker-id "] " job-str text)]
     (log level text))))

;; (default-logger {:worker-id "foo-worker"} {:queue "baz-q" :id 1 :name "bar-job"} "hello" :info)

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
(defn reserve-and-run-one-job
  "If a job can be reserved returns the result of the run (:success
  or :fail), otherwise returns nil. If configured will throw on too
  many reserve errors."
  [{:keys [logger on-reserve-fail max-failed-reserve-count failed-reserved-count worker-status] :as env}]
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
                  :stop (reset! worker-status :stopped)
                  :throw (throw (ex-info "Failed to reserve jobs"
                                         {:last-exception e
                                          :failed-reserved-count @failed-reserved-count}))))))))
      :fail)))

(defn run-job-batch
  "Run a number of jobs in one batch, consecutively. Return map of
  success and fail count. We're running in a thread, so we check
  worker status and stop processing the batch if worker status in
  not :running"
  [{:keys [exit-on-complete? poll-batch-size worker-status logger] :as env}]
  (loop [result-count {:success 0 :fail 0}
         counter 0]
    (if-let [success-or-fail (and (= @worker-status :running)
                                  (< counter poll-batch-size)
                                  (reserve-and-run-one-job env))]
      (recur (update result-count success-or-fail inc)
             (inc counter))
      result-count)))

