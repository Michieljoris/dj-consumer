(ns dj-consumer.worker-test
  (:require
   [dj-consumer.database.core :as db]
   [dj-consumer.database.connection :as db-conn]
   [dj-consumer.fixtures :as fixtures]
   [clojure.test :as t :refer [deftest is use-fixtures testing]]

   [clj-time.core :as time]
   [clj-time.coerce :as time-coerce]
   [clj-time.local :as time-local]
   [dj-consumer.util :as u]
   [dj-consumer.job :as job]
   [clojure.set :as set]

   [dj-consumer.worker :as tn]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   ))

(def defaults
  (let [test-db-name "dj_consumer_test"
        job-table :job-table
        db-config {:user "root"
                   :password ""
                   :url "//localhost:3306/"
                   :schema {:job-table {:priority :int
                                        :id :int
                                        :attempts :int
                                        :handler :long-text
                                        :last-error :long-text
                                        :run-at :date-time
                                        :locked-at :date-time
                                        :failed-at :date-time
                                        :locked-by :text
                                        :queue :text}}
                   :db-name test-db-name}]
    {:test-db-name test-db-name
     :job-table job-table
     :db-config db-config
     :mysql-conn (db-conn/make-db-conn (assoc db-config :db-name "mysql"))
     :db-conn (db-conn/make-db-conn db-config)
     :worker-config {:db-config db-config
                     :worker-id :sample-worker
                     :table job-table
                     :sql-log? true}
     :default-job {:priority 0 :attempts 0 :handler nil :last-error nil
                   :run-at nil :locked-at nil :failed-at nil :locked-by nil :queue nil}}))

(defn prepare-for-test [defaults fixtures]
  (let [worker (tn/make-worker (:worker-config defaults))
        env (tn/env worker)
        ;; env (assoc env :tz-offset (u/get-local-tz-offset))
        ]
    {:env env
     :worker worker
     :fixtures (if (seq fixtures)
                 (get (fixtures/setup-job-test-db defaults fixtures) (:job-table defaults)))}))

(defn job-table-data [{:keys [table]
                       ;; {:keys [hours minutes]} :tz-offset
                       {:keys [schema]} :db-config
                       :as env}]
  (let [jobs (db/sql env :select-all-from {:table table})
        schema (table schema)]

    (map (fn [job]
           (into {} (map (fn [[col v]]
                           [col (cond-> v
                                  (and (some? v) (= :date-time (col schema)))
                                  (time/to-time-zone  (time/default-time-zone)
                                   ;; (time/time-zone-for-offset hours minutes)
                                   )
                                  )]
                           )) job))
         jobs)))

(deftest test-fixtures
  (let [{:keys [env worker fixtures]}
        (prepare-for-test defaults [{:locked-at nil}])]
    (testing "Simple test to test if fixtures are working :)"
      (is (= (job-table-data env) fixtures)))))

(deftest default-logger
  (is (= 1 1
       ;; (tn/default-logger {:worker-id "some-worker" :table :job-table}
       ;;                    {:id 1 :name :some-job :queue "some-queue"} :info "some text")
       ;; "foo"
       )))

(deftest clear-locks
  (let [some-time (u/now)
        {:keys [env worker fixtures]}
        (prepare-for-test defaults [{:locked-at (u/sql-time some-time) :locked-by "sample-worker"}
                                    {:locked-at (u/sql-time some-time) :locked-by "sample-worker"}
                                    {:locked-at (u/sql-time some-time) :locked-by "sample-worker2"}
                                    ])]
    (tn/clear-locks env)
    (is
     (=
      (job-table-data env)
      [{:locked-by nil, :locked-at nil, :id 1}
       {:locked-by nil, :locked-at nil, :id 2}
       {:locked-by "sample-worker2", :locked-at (u/sql-time some-time) :id 3}]))))

(defn test-logger
  ([env level text]
   (default-logger env nil level text))
  ([{:keys [worker-id log-atom]} {:keys [id name queue] :as job} level text]
   (let [queue-str (if queue (str ", queue=" queue))
         job-str (if job (str "Job " name " (id=" id queue-str ") " ))
         text (str "[" worker-id "] " job-str text)]
     (swap! log-atom #(conj % {:level level :text text})))))

(defmethod job/failed :test-failed [_ {:keys [invoked?] :as job}]
  (reset! invoked? true))

(defmethod job/failed :test-thrown [_ {:keys [invoked? name]}]
  (reset! invoked? true)
  (throw (ex-info "job has thrown " {:job-name name})))

(deftest failed
  (let [{:keys [env worker fixtures]}
        (prepare-for-test defaults [{:priority 0 :failed-at nil :attempts 1}
                                    {:priority 1 :failed-at nil}
                                    ])
        job {:name :test-failed :id 1 :invoked? (atom false)}
        now (u/now)]
    (with-redefs [dj-consumer.util/now (constantly now)]
      (tn/failed env job)
      (is  @(:invoked? job) "job is invoked")
      (is (=
           (job-table-data env)
           [{:attempts 1,
             :failed-at (u/sql-time now) ,
             :priority 0,
             :id 1}
            {:attempts 0, :failed-at nil, :priority 1, :id 2}])
          "job record's failed-at is set properly")))

  (let [log-atom (atom [])
        {:keys [env worker fixtures]}
        (prepare-for-test (update defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:delete-failed-jobs? true
                                            :log-atom log-atom
                                            :logger test-logger})
                                    ))
                          [{:priority 0 :failed-at nil :attempts 1}
                           {:priority 1 :failed-at nil}
                           {}])
        job1 {:name :job1 :id 1 :fail-reason "foo"}
        job2 {:name :job2 :id 2 :delete-if-failed? false :fail-reason "bar"}
        job3 {:name :test-thrown :id 3 :invoked? (atom false)}
        now (u/now)]
    (with-redefs [dj-consumer.util/now (constantly now)
                  dj-consumer.util/exception-str (fn [e] "some-exception-str" )]
      (tn/failed env job1)
      (tn/failed env job2)
      (tn/failed env job3)
      (is  @(:invoked? job3) "job is invoked")
      (is (=
           (job-table-data env)
           [{:attempts 0, :failed-at (u/sql-time now), :priority 1, :id 2}])
          "job record's is removed when failed when worker is configured to do so, but job can override this. If jobs' fail callback throws exception, job is still removed, but this is logged")
      (is (= @log-atom [{:level :error,
                         :text
                         "[sample-worker] Job :job1 (id=1) REMOVED permanently because of foo"}
                        {:level :error,
                         :text
                         "[sample-worker] Job :job2 (id=2) MARKED failed because of bar"}
                        {:level :error,
                         :text
                         "[sample-worker] Job :test-thrown (id=3) Exception when running fail callback:\nsome-exception-str"}
                        {:level :error,
                         :text
                         "[sample-worker] Job :test-thrown (id=3) REMOVED permanently because of "}])
          "fails are logged"))))

(deftest reschedule
  (let [log-atom (atom [])
        now (u/now)
        {:keys [env worker fixtures]}
        (prepare-for-test (update defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:delete-failed-jobs? true
                                            :log-atom log-atom
                                            :reschedule-at (fn [some-time attempts] some-time)
                                            :logger test-logger})))
                          [{:locked-by "somebody" :locked-at now :run-at nil :failed-at nil :attempts nil}])
        job {:name :job1 :id 1 :attempts 1}]
    (with-redefs [dj-consumer.util/now (constantly now)]
      (tn/reschedule env job)
      (is (=
           (job-table-data env)
           [{:locked-by nil,
             :attempts 1,
             :failed-at nil,
             :run-at (u/sql-time now) ,
             :locked-at nil,
             :id 1}])
          "Attempts and run-at cols are set. ")
      (is (= @log-atom [{:level :info,
                         :text
                         (str "[sample-worker] Job :job1 (id=1) Rescheduled at " (u/time->str now))}])
          "reschedule is logged"))))

(defmethod job/run :test-job [_ {:keys [invoked? throw-run? sleep-run name]}]
  (swap! invoked? conj :run)
  (if sleep-run (Thread/sleep sleep-run))
  (if throw-run? (throw (ex-info "run hook has thrown" {:job-name name}))))

(defmethod job/exception :test-job [_ {:keys [invoked? throw-exception? name]}]
  (swap! invoked? conj :exception)
  (if throw-exception? (throw (ex-info "exception hook has thrown" {:job-name name}))))

(defmethod job/finally :test-job [_ {:keys [invoked? throw-finally? name sleep-finally]}]
  (swap! invoked? conj :finally)
  (if sleep-finally (Thread/sleep sleep-finally))
  (if throw-finally? (throw (ex-info "finally hook has thrown" {:job-name name}))))

(deftest invoke-job
  (let [log-atom (atom [])
        {:keys [env worker fixtures]}
        (prepare-for-test (update defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:log-atom log-atom
                                            :logger test-logger})))
                          nil)
        invoked? (atom [])
        job {:name :test-job :id 1 :invoked? invoked?}]
    (tn/invoke-job job)
    (is (= @invoked? [:run :finally])
        "Without exceptions run and finally hooks are run for a job")
    (is (= @log-atom [])
        "and nothing is logged")

    (reset! invoked? [])
    (is (thrown-with-msg? Exception #"run"
                          (tn/invoke-job (assoc job :throw-run? true)))
        "run hook throws exception and which is rethrown")
    (is (= @invoked? [:run :exception :finally])
        "With exception in run,  exception hook is also run")
    (is (= @log-atom [])
        "and still nothing is logged")

    (reset! invoked? [])
    (is (thrown-with-msg? Exception #"exception"
                          (tn/invoke-job (assoc job :throw-run? true
                                                :throw-exception? true))
                          )
        "exception hook throws exception, which is not caught")
    (is (= @invoked? [:run :exception :finally])
        "With exception in run and in exception hook, all 3 hooks are still run")
    (is (= @log-atom [])
        "and still nothing is logged")

    (reset! invoked? [])
    (is (thrown-with-msg? Exception #"finally"
                          (tn/invoke-job (assoc job :throw-finally? true))
                          )
        "finally hook throws exception, which is not caught")
    (is (= @invoked? [:run :finally])
        "Exception in finally is not caught")
    (is (= @log-atom [])
        "and still nothing is logged")))

(deftest invoke-job-with-timeout
  (let [log-atom (atom [])
        {:keys [env worker fixtures]}
        (prepare-for-test (update defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:log-atom log-atom
                                            :logger test-logger})))
                          nil)
        invoked? (atom [])
        job {:name :test-job :id 1 :invoked? invoked? :max-run-time 50}]
    (tn/invoke-job-with-timeout job)
    (is (= @invoked? [:run :finally])
        "Without exceptions run and finally hooks are run normally for a job that doesn't timeout")
    (is (= @log-atom [])
        "and nothing is logged")

    (reset! invoked? [])
    (is (thrown-with-msg? Exception #"run"
                          (tn/invoke-job-with-timeout (assoc job :throw-run? true)))
        "exceptions in hooks are still thrown properly")
    (is (= @invoked? [:run :exception :finally])
        "proper hooks are all still run in job that doesn't time out")
    (is (= @log-atom [])
        "and still nothing is logged")

    (reset! invoked? [])
    (is (thrown-with-msg? Exception #"has timed out"
                          (tn/invoke-job-with-timeout (assoc job :sleep-run 100)))
        "timeout exception is trown if run sleeps for more than max-run-time for job")
    (is (= @invoked? [:run])
        "Only run hook is invoked so far because job timed out while in run method.")
    (Thread/sleep 100)
    (is (= @invoked? [:run :finally])
        "After waiting some more, finally hook is still run!! Even though job has timed out.")
    (is (= @log-atom [])
        "and still nothing is logged")

    (reset! invoked? [])
    (is (thrown-with-msg? Exception #"has timed out"
                          (tn/invoke-job-with-timeout (assoc job :run-sleep 20 :sleep-finally 80)))
        "")
    (is (= @invoked? [:run :finally])
        "Run and finally hook are invoked, timeout occurred during finally call,  but job still timed out")
    (is (= @log-atom [])
        "and still nothing is logged")))

(deftest handle-run-exception
  (let [log-atom (atom [])
        {:keys [env worker fixtures]}
        (prepare-for-test (update defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:delete-failed-jobs? true
                                            :log-atom log-atom
                                            :max-attempts 3
                                            :logger test-logger})
                                    ))
                          [{:priority 0 :failed-at nil :attempts 1}
                           {:priority 1 :failed-at nil}
                           {}])
        handle-called (atom {})
        job1 {:name :job1 :id 1 :attempts 2}
        some-exception (Exception. "job1 exception")
        fail-exception (ex-info "job1 exception" {:failed? true})
        timeout-exception (ex-info "job1 exception" {:timed-out? true})
        timed-out? (atom nil)
        ]
    (with-redefs [dj-consumer.util/exception-str (fn [e] (.getMessage e))
                  dj-consumer.worker/reschedule (fn [env job]
                                                  (swap! handle-called assoc :reschedule job))
                  dj-consumer.worker/failed (fn [env job]
                                              (swap! handle-called assoc :failed job))
                  ]
      (tn/handle-run-exception env job1 some-exception)
      (is (= @log-atom
             [{:level :error,
               :text
               "[sample-worker] Job :job1 (id=1) FAILED to run. Failed 3 attempts. Last exception:\njob1 exception"}])
          "Too many attempts as set in env. Exception is logged")
      (is (= @handle-called {:failed {:name :job1, :id 1, :attempts 3,
                                      :exception some-exception,
                                      :fail-reason "3 consecutive failures"}})
          "failed is called, with proper fail-reason and the exception assoced to job")

      (reset! log-atom [])
      (reset! handle-called {})
      (tn/handle-run-exception env (assoc job1 :attempts 1 :max-attempts 2) some-exception)
      (is (= @log-atom
             [{:level :error,
               :text
               "[sample-worker] Job :job1 (id=1) FAILED to run. Failed 2 attempts. Last exception:\njob1 exception"}])
          "Too many attempts as set in job. Exception is logged")
      (is (= @handle-called {:failed {:name :job1, :id 1, :attempts 2, :max-attempts 2
                                      :exception some-exception,
                                      :fail-reason "2 consecutive failures"}})
          "failed is called, with proper fail-reason and the exception assoced to job")

      (reset! log-atom [])
      (reset! handle-called {})
      (tn/handle-run-exception env (assoc job1 :attempts 1) fail-exception)
      (is (= @log-atom
             [{:level :error,
               :text
               "[sample-worker] Job :job1 (id=1) FAILED to run. Job requested fail Last exception:\njob1 exception"}])
          "Job requests fail by setting :failed? true to context of exception")
      (is (= @handle-called {:failed {:name :job1, :id 1, :attempts 2
                                      :exception fail-exception
                                      :fail-reason "job requested to be failed"}})
          "failed is called, with proper fail-reason and the exception assoced to job")

      (reset! log-atom [])
      (reset! handle-called {})
      (tn/handle-run-exception env (assoc job1
                                          :attempts 2
                                          :timed-out? timed-out?)
                               timeout-exception)
      (is (= @log-atom
             [{:level :error,
               :text
               "[sample-worker] Job :job1 (id=1) FAILED to run. Failed 3 attempts. Last exception:\njob1 exception"}])
          "timeout exception")
      (is (= @handle-called {:failed {:name :job1, :id 1, :attempts 3
                                      :timed-out? timed-out?
                                      :exception timeout-exception
                                      :fail-reason "3 consecutive failures"}})
          "failed is called, with proper fail-reason and the exception assoced to job")
      (is (= @timed-out? true) "timed-out? atom on job is set to true")

      (reset! log-atom [])
      (reset! handle-called {})
      (reset! timed-out? false)
      (tn/handle-run-exception env (assoc job1
                                          :attempts 1
                                          :timed-out? timed-out?)
                               some-exception)
      (is (= @log-atom [{:level :error,
                         :text
                         "[sample-worker] Job :job1 (id=1) FAILED to run.  Last exception:\njob1 exception"}]
             )
          "some exception is logged properly")
      (is (= @handle-called {:reschedule
                             {:name :job1,
                              :id 1,
                              :attempts 2,
                              :timed-out? timed-out?}})
          "attempts is less than max-attempts and no failed? is requested from job: rescheduled is called")
      (is (= @timed-out? false) "timed-out? atom on job is still set set to nil"))))

(deftest run
  (let [log-atom (atom [])
        {:keys [env worker fixtures]}
        (prepare-for-test (update defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:delete-failed-jobs? true
                                            :log-atom log-atom
                                            :max-attempts 3
                                            :logger test-logger})
                                    ))
                          [{:priority 0 :failed-at nil :attempts 1}
                           {:priority 1}
                           {}])
        fn-called (atom {})
        job1 {:name :job1 :id 1 :attempts 2}
        job2 {:name :job2 :id 2 :attempts 2}
        some-exception (Exception. "job1 exception")]
    (with-redefs [dj-consumer.util/runtime (fn [f] {:runtime 1})
                  dj-consumer.worker/invoke-job-with-timeout (fn [job]
                                                               (swap! fn-called assoc :invoke-job-with-timeout job)
                                                               (if (= (:priority job) 1)
                                                                 (throw some-exception))
                                                               )
                  dj-consumer.worker/handle-run-exception  (fn [env job e]
                                                             (swap! fn-called assoc
                                                                    :handle-run-exception job
                                                                    :exception e
                                                                    ))
                  ]

      (is (= (job-table-data env) [{:attempts 1 :failed-at nil :priority 0 :id 1}
                                   {:attempts 0, :failed-at nil, :priority 1, :id 2}
                                   {:attempts 0, :failed-at nil, :priority 0, :id 3}])
          "all fixtures jobs are in job table")
      (is (= (tn/run env job1) :success)
          "No exception thrown in job, returns :true")
      (is (= @log-atom [{:level :info, :text "[sample-worker] Job :job1 (id=1) RUNNING"}
                        {:level :info,
                         :text "[sample-worker] Job :job1 (id=1) COMPLETED after 1ms"}])
          "job start run and completed are logged")
      (is (= @fn-called {:invoke-job-with-timeout {:name :job1, :id 1, :attempts 2}})
          "invoke-job-with-timeout is invoked with job")
      (is (= (job-table-data env) [{:attempts 0, :failed-at nil, :priority 1, :id 2}
                                   {:attempts 0, :failed-at nil, :priority 0, :id 3}])
          "job1 is deleted")

      (reset! log-atom [])
      (reset! fn-called {})
      (is (= (tn/run env (assoc job2 :priority 1)) :fail)
          "Exception is thrown in job run, result is :fail")
      (is (= @log-atom [{:level :info, :text "[sample-worker] Job :job2 (id=2) RUNNING"}])
          "Only job RUNNING is logged")
      (is (= @fn-called {:invoke-job-with-timeout {:name :job2, :id 2, :attempts 2, :priority 1}
                         :handle-run-exception {:name :job2 :id 2, :attempts 2 :priority 1}
                         :exception some-exception})
          "both invoke-job and handle-run-exception are called")
      (is (= (job-table-data env) [{:attempts 0, :failed-at nil, :priority 1, :id 2}
                                   {:attempts 0, :failed-at nil, :priority 0, :id 3}])
          "job2 is not deleted"))))

(deftest reserve-job
  (let [log-atom (atom [])
        {:keys [env worker fixtures]}
        (prepare-for-test (update defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:log-atom log-atom
                                            :logger test-logger})
                                    ))
                          [{:run-at nil :failed-at nil
                            :locked-at nil :locked-by nil
                            :queue nil :priority 0}
                           {}
                           {}])
        fn-called (atom {})
        some-exception (Exception. "job1 exception")]
    (with-redefs [dj-consumer.util/runtime (fn [f] {:runtime 1})
                  dj-consumer.worker/invoke-job-with-timeout (fn [job]
                                                               (swap! fn-called assoc :invoke-job-with-timeout job)
                                                               (if (= (:priority job) 1)
                                                                 (throw some-exception))
                                                               )
                  dj-consumer.worker/handle-run-exception  (fn [env job e]
                                                             (swap! fn-called assoc
                                                                    :handle-run-exception job
                                                                    :exception e
                                                                    ))
                  ]

      ))
  )

(deftest reserve-and-run-one-job)

(deftest run-job-batch)

(deftest stop-worker)

(deftest start-worker)

(deftest make-worker)

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
