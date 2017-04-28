(ns dj-consumer.reserve-and-run-test
  (:require [clj-time.core :as time]
            [clojure.core.async :as async]
            [clojure.test :as t :refer [deftest is testing]]
            [dj-consumer
             [test-util :as tu]
             [job :as job]
             [reserve-and-run :as tn]
             [worker :as worker]]
            [digicheck.util :as u]
            [dj-consumer.database
             [connection :as db-conn]
             [core :as db]]
            
            [taoensso.timbre :as timbre
             :refer (log  trace  debug  info  warn fatal  report color-str
                          logf tracef debugf infof warnf errorf fatalf reportf
                          spy get-env log-env)]))

;;TODO
(deftest default-logger
  (is (= 1 1
       ;; (tn/default-logger {:worker-id "some-worker" :table :job-table}
       ;;                    {:id 1 :name :some-job :queue "some-queue"} :info "some text")
       ;; "foo"
       )))

(deftest clear-locks
  (let [some-time (u/now)
        {:keys [env worker fixtures]}
        (tu/prepare-for-test tu/defaults [{:locked-at (u/sql-time some-time) :locked-by "sample-worker"}
                                    {:locked-at (u/sql-time some-time) :locked-by "sample-worker"}
                                    {:locked-at (u/sql-time some-time) :locked-by "sample-worker2"}])]
    (tn/clear-locks env)
    (is
     (=
      (tu/job-table-data env)
      [{:locked-by nil, :locked-at nil, :id 1}
       {:locked-by nil, :locked-at nil, :id 2}
       {:locked-by "sample-worker2", :locked-at (u/sql-time some-time) :id 3}]))))

(defmethod job/failed :test-failed [{:keys [invoked?] :as job}]
  (reset! invoked? true))

(defmethod job/failed :test-thrown [{:keys [invoked? name]}]
  (reset! invoked? true)
  (throw (ex-info "job has thrown " {:job-name name})))

(deftest failed
  (let [{:keys [env worker fixtures]}
        (tu/prepare-for-test tu/defaults [{:priority 0 :failed-at nil :attempts 0}
                                          {:priority 1 :failed-at nil}
                                          ])
        job {:name :test-failed :id 1 :attempts 1 :invoked? (atom false)}
        now (u/now)]
    (with-redefs [digicheck.util/now (constantly now)]
      (tn/failed env job)
      (is  @(:invoked? job) "job failed hook is invoked")
      (is (=
           (tu/job-table-data env)
           [{:attempts 1,
             :failed-at (u/sql-time now) ,
             :priority 0,
             :id 1}
            {:attempts 0, :failed-at nil, :priority 1, :id 2}])
          "job record's failed-at is set properly;; ")))

  (let [log-atom (atom [])
        {:keys [env worker fixtures]}
        (tu/prepare-for-test (update tu/defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:delete-failed-jobs? true
                                            :log-atom log-atom
                                            :logger tu/test-logger})
                                    ))
                          [{:priority 0 :failed-at nil :attempts 0}
                           {:priority 1 :failed-at nil}
                           {}])
        job1 {:name :job1 :id 1 :fail-reason "foo"}
        job2 {:name :job2 :id 2 :delete-if-failed? false :attempts 2 :fail-reason "bar"}
        job3 {:name :test-thrown :id 3 :invoked? (atom false)}
        now (u/now)]
    (with-redefs [digicheck.util/now (constantly now)
                  digicheck.util/exception-str (fn [e] "some-exception-str" )]
      (tn/failed env job1)
      (tn/failed env job2)
      (tn/failed env job3)
      (is  @(:invoked? job3) "job is invoked")
      (is (=
           (tu/job-table-data env)
           [{:attempts 2, :failed-at (u/sql-time now), :priority 1, :id 2}])
          "job record's is removed when failed when worker is configured to do so, but job can override this. If jobs' fail callback throws exception, job is still removed, but this is logged")
      (is (= @log-atom [":error-[sample-worker] Job :job1 (id=1) REMOVED permanently because of foo"
                        ":error-[sample-worker] Job :job2 (id=2) MARKED failed because of bar"
                        ":error-[sample-worker] Job :test-thrown (id=3) Exception when running fail callback:\nsome-exception-str"
                        ":error-[sample-worker] Job :test-thrown (id=3) REMOVED permanently because of "])
          "fails are logged"))))

(deftest reschedule
  (let [log-atom (atom [])
        now (u/now)
        {:keys [env worker fixtures]}
        (tu/prepare-for-test (update tu/defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:delete-failed-jobs? true
                                            :log-atom log-atom
                                            :reschedule-at (fn [some-time attempts] some-time)
                                            :logger tu/test-logger})))
                          [{:locked-by "somebody" :locked-at now :run-at nil :failed-at nil :attempts nil}])
        job {:name :job1 :id 1 :attempts 1}]
    (with-redefs [digicheck.util/now (constantly now)]
      (tn/reschedule env job)
      (is (=
           (tu/job-table-data env)
           [{:locked-by nil,
             :attempts 1,
             :failed-at nil,
             :run-at (u/sql-time now) ,
             :locked-at nil,
             :id 1}])
          "Attempts and run-at cols are set. ")
      (is (= @log-atom [(str ":info-[sample-worker] Job :job1 (id=1) Rescheduled at "
                             (u/time->str now))])
          "reschedule is logged"))))

(defmethod job/run :test-job [{:keys [invoked? throw-run? sleep-run name]}]
  (swap! invoked? conj :run)
  (if sleep-run (Thread/sleep sleep-run))
  (if throw-run? (throw (ex-info "run hook has thrown" {:job-name name}))))

(defmethod job/exception :test-job [{:keys [invoked? throw-exception? name]}]
  (swap! invoked? conj :exception)
  (if throw-exception? (throw (ex-info "exception hook has thrown" {:job-name name}))))

(defmethod job/finally :test-job [{:keys [invoked? throw-finally? name sleep-finally]}]
  (swap! invoked? conj :finally)
  (if sleep-finally (Thread/sleep sleep-finally))
  (if throw-finally? (throw (ex-info "finally hook has thrown" {:job-name name}))))

(deftest invoke-job
  (let [log-atom (atom [])
        {:keys [env worker fixtures]}
        (tu/prepare-for-test (update tu/defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:log-atom log-atom
                                            :logger tu/test-logger})))
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
        (tu/prepare-for-test (update tu/defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:log-atom log-atom
                                            :logger tu/test-logger})))
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
        (tu/prepare-for-test (update tu/defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:delete-failed-jobs? true
                                            :log-atom log-atom
                                            :max-attempts 3
                                            :logger tu/test-logger})
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
    (with-redefs [digicheck.util/exception-str (fn [e] (.getMessage e))
                  dj-consumer.reserve-and-run/reschedule (fn [env job]
                                                  (swap! handle-called assoc :reschedule job))
                  dj-consumer.reserve-and-run/failed (fn [env job]
                                              (swap! handle-called assoc :failed job))
                  ]
      (tn/handle-run-exception env job1 some-exception)
      (is (= @log-atom
             [":error-[sample-worker] Job :job1 (id=1) FAILED to run. Failed 3 attempts. Last exception:\njob1 exception"])
          "Too many attempts as set in env. Exception is logged")
      (is (= @handle-called {:failed {:name :job1, :id 1, :attempts 3,
                                      :exception some-exception,
                                      :fail-reason "3 consecutive failures"}})
          "failed is called, with proper fail-reason and the exception assoced to job")

      (reset! log-atom [])
      (reset! handle-called {})
      (tn/handle-run-exception env (assoc job1 :attempts 1 :max-attempts 2) some-exception)
      (is (= @log-atom
             [":error-[sample-worker] Job :job1 (id=1) FAILED to run. Failed 2 attempts. Last exception:\njob1 exception"]
             )
          "Too many attempts as set in job. Exception is logged")
      (is (= @handle-called {:failed {:name :job1, :id 1, :attempts 2, :max-attempts 2
                                      :exception some-exception,
                                      :fail-reason "2 consecutive failures"}})
          "failed is called, with proper fail-reason and the exception assoced to job")

      (reset! log-atom [])
      (reset! handle-called {})
      (tn/handle-run-exception env (assoc job1 :attempts 1) fail-exception)
      (is (= @log-atom
             [":error-[sample-worker] Job :job1 (id=1) FAILED to run. Job requested fail Last exception:\njob1 exception"])
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
             [":error-[sample-worker] Job :job1 (id=1) FAILED to run. Failed 3 attempts. Last exception:\njob1 exception"])
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
      (is (= @log-atom
             [":error-[sample-worker] Job :job1 (id=1) FAILED to run.  Last exception:\njob1 exception"])
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
        (tu/prepare-for-test (update tu/defaults :worker-config
                                  (fn [worker-config]
                                    (merge worker-config
                                           {:delete-failed-jobs? true
                                            :log-atom log-atom
                                            :max-attempts 3
                                            :logger tu/test-logger})
                                    ))
                          [{:priority 0 :failed-at nil :attempts 1}
                           {:priority 1}
                           {}])
        fn-called (atom {})
        job1 {:name :job1 :id 1 :attempts 2}
        job2 {:name :job2 :id 2 :attempts 2}
        some-exception (Exception. "job1 exception")]
    (with-redefs [digicheck.util/runtime (fn [f & args] (apply f args)
                                             {:runtime 1})
                  dj-consumer.reserve-and-run/invoke-job-with-timeout
                  (fn [job]
                    (swap! fn-called assoc :invoke-job-with-timeout job)
                    (if (= (:priority job) 1)
                      (throw some-exception)))
                  dj-consumer.reserve-and-run/handle-run-exception
                  (fn [env job e]
                    (swap! fn-called assoc
                           :handle-run-exception job
                           :exception e))]
      (is (= (tu/job-table-data env) [{:attempts 1 :failed-at nil :priority 0 :id 1}
                                   {:attempts 0, :failed-at nil, :priority 1, :id 2}
                                   {:attempts 0, :failed-at nil, :priority 0, :id 3}])
          "all fixtures jobs are in job table")
      (is (= (tn/run env job1) :success)
          "No exception thrown in job, returns :true")
      (is (= @log-atom
             [":info-[sample-worker] Job :job1 (id=1) RUNNING"
              ":info-[sample-worker] Job :job1 (id=1) COMPLETED after 1ms"])
          "job start run and completed are logged")
      (is (= @fn-called {:invoke-job-with-timeout {:name :job1, :id 1, :attempts 2}})
          "invoke-job-with-timeout is invoked with job")
      (is (= (tu/job-table-data env) [{:attempts 0, :failed-at nil, :priority 1, :id 2}
                                   {:attempts 0, :failed-at nil, :priority 0, :id 3}])
          "job1 is deleted")

      (reset! log-atom [])
      (reset! fn-called {})
      (is (= (tn/run env (assoc job2 :priority 1)) :fail)
          "Exception is thrown in job run, result is :fail")
      (is (= @log-atom [":info-[sample-worker] Job :job2 (id=2) RUNNING"])
          "Only job RUNNING is logged")
      (is (= @fn-called {:invoke-job-with-timeout {:name :job2, :id 2, :attempts 2, :priority 1}
                         :handle-run-exception {:name :job2 :id 2, :attempts 2 :priority 1}
                         :exception some-exception})
          "both invoke-job and handle-run-exception are called")
      (is (= (tu/job-table-data env) [{:attempts 0, :failed-at nil, :priority 1, :id 2}
                                   {:attempts 0, :failed-at nil, :priority 0, :id 3}])
          "job2 is not deleted"))))

(deftest parse-ruby-yaml
  (is (= (tn/parse-ruby-yaml
          "--- !ruby/struct:InvitationExpirationReminderJob
invitation_id: 882\nfoo: bar")
         {:name :invitation-expiration-reminder-job,
          :payload {:invitation_id 882,
                    :foo "bar"}})
      "extract proper name and payload if job handler is a struct")


  (is (=
       (tn/parse-ruby-yaml
        "---
  object:
  raw_attributes:
    id: 1
    nested:
      p1: 2 ")
       {:name :unknown-job-name,
        :payload {:object nil, :raw_attributes {:id 1, :nested {:p1 2}}}})
      "Parse yaml properly")

  (is (=
       (tn/parse-ruby-yaml
        "---
  object: !ruby/object:User
  method_name: foo !ruby/bla-bla
  raw_attributes:
    id: 1
    nested:
      p1: 2 ")
       {:name :user/foo,
        :payload {:object nil,
                  :method_name "foo",
                  :raw_attributes {:id 1, :nested {:p1 2}}}})
      "Ignore ruby anotations, extract ruby object name and method and set on :name key and set payload")

  (is (thrown? Exception (tn/parse-ruby-yaml "foo: bar\nbaz foo"))
      "Incorrect yaml throws exception"))

(def u-now (u/now))
(def now (u/sql-time u-now))
(def a-minute-ago (time/minus now (time/minutes 1)))
(def two-minutes-ago (time/minus now (time/minutes 2)))
(def three-minutes-ago (time/minus now (time/minutes 3)))
(def five-hours-ago (time/minus now (time/hours 5)))

(deftest reserve-job
  (with-redefs [digicheck.util/now (constantly u-now)]
    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {} [])]
      (let [job (tn/reserve-job env)]
        (is (= job nil)
            "return nil if no job is found")
        (is (empty? (tu/job-table-data env))
            "job table is still empty")))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {}
                                                [{:run-at a-minute-ago
                                                  :failed-at nil
                                                  :locked-at nil :locked-by nil
                                                  :queue nil :priority nil}])]
      (let [job (tn/reserve-job env)
            job (update job :timed-out? deref)]
        (is (= (tu/adjust-tz env job) {:payload nil,
                                    :queue nil,
                                    :name :unknown-job-name,
                                    :locked-by "sample-worker",
                                    :timed-out? false,
                                    :max-run-time 14400,
                                    :failed-at nil,
                                    :priority nil,
                                    :id 1,
                                    :run-at a-minute-ago,
                                    :locked-at now})
            "Finds job that has run-at<now")
        (is (= (tu/job-table-data env) [{:queue nil,
                                      :locked-by "sample-worker",
                                      :locked-at now
                                      :failed-at nil,
                                      :priority nil,
                                      :run-at a-minute-ago
                                      :id 1}])
            "Job is locked")))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {:min-priority 1
                                                          :max-priority 10}
                                                [{:run-at a-minute-ago
                                                  :failed-at nil
                                                  :locked-at nil :locked-by nil
                                                  :queue nil :priority nil}
                                                 {:run-at a-minute-ago
                                                  :priority 0}
                                                 {:run-at a-minute-ago
                                                  :priority 1}
                                                 {:run-at two-minutes-ago
                                                  :priority 1}])]
      (let [job (tn/reserve-job env)
            job (update job :timed-out? deref)]
        (is (= (tu/adjust-tz env job) {:payload nil,
                                    :queue nil,
                                    :name :unknown-job-name,
                                    :locked-by "sample-worker",
                                    :timed-out? false,
                                    :max-run-time 14400,
                                    :failed-at nil,
                                    :priority 1,
                                    :id 4,
                                    :run-at two-minutes-ago,
                                    :locked-at now})
            "Finds job that has lowest run-at and run-at<now and lowest priority within range")
        (is (= (tu/job-table-data env) (update-in fixtures [3]
                                               (fn [job]
                                                 (assoc job
                                                        :locked-at now
                                                        :locked-by "sample-worker"))))
            "Job is locked")))
    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {:min-priority 1
                                                          :max-priority 10}
                                                [{:run-at two-minutes-ago
                                                  :failed-at nil
                                                  :locked-at nil :locked-by nil
                                                  :queue nil :priority nil}
                                                 {:run-at a-minute-ago
                                                  :priority 0}
                                                 {:run-at a-minute-ago
                                                  :priority 11}])]
      (let [job (tn/reserve-job env)]
        (is (= job nil)
            "No job is within min and max priority ")
        (is (= (tu/job-table-data env) fixtures)
            "Job table is not changed")))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {:queues ["q1" "q2"]}
                                                [{:run-at three-minutes-ago
                                                  :failed-at nil
                                                  :locked-at nil :locked-by nil
                                                  :queue nil :priority nil}
                                                 {:run-at a-minute-ago
                                                  :queue "q1"}
                                                 {:run-at two-minutes-ago
                                                  :queue "q2" }
                                                 {:run-at three-minutes-ago
                                                  :queue "q3" :priority 0}
                                                 ])]
      (let [job (tn/reserve-job env)
            job (update job :timed-out? deref)]
        (is (= (tu/adjust-tz env job) {:payload nil,
                                    :queue "q2",
                                    :name :unknown-job-name,
                                    :locked-by "sample-worker",
                                    :timed-out? false,
                                    :max-run-time 14400,
                                    :failed-at nil,
                                    :priority 0,
                                    :id 3,
                                    :run-at two-minutes-ago
                                    :locked-at now})
            "Select job from queues as set for worker picking the older run-at, ignoring jobs with
              higher priority and/or older run-at, but wrong queue")
        (is (= (tu/job-table-data env) (update-in fixtures [2]
                                               (fn [job]
                                                 (assoc job
                                                        :locked-at now
                                                        :locked-by "sample-worker"))))
            "Job is locked")))
    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {:queues [nil]}
                                                [{:run-at two-minutes-ago
                                                  :failed-at nil
                                                  :locked-at nil :locked-by nil
                                                  :queue nil :priority nil}
                                                 {:run-at a-minute-ago
                                                  :queue nil}
                                                 {:run-at three-minutes-ago
                                                  :queue "q" :priority 0}
                                                 ])]
      (let [job (tn/reserve-job env)
            job (update job :timed-out? deref)]
        (is (= (tu/adjust-tz env job) {:payload nil,
                                    :queue nil,
                                    :name :unknown-job-name,
                                    :locked-by "sample-worker",
                                    :timed-out? false,
                                    :max-run-time 14400,
                                    :failed-at nil,
                                    :priority nil,
                                    :id 1,
                                    :run-at two-minutes-ago
                                    :locked-at now})
            "[nil] for queues ignores jobs with named queue")
        (is (= (tu/job-table-data env) (update-in fixtures [0]
                                               (fn [job]
                                                 (assoc job
                                                        :locked-at now
                                                        :locked-by "sample-worker"))))
            "Job is locked")))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {:queues [nil "q1"]}
                                                [{:run-at two-minutes-ago
                                                  :failed-at nil
                                                  :locked-at nil :locked-by nil
                                                  :queue nil :priority 2}
                                                 {:run-at a-minute-ago
                                                  :queue "q1" :priority 1}
                                                 {:run-at three-minutes-ago
                                                  :queue "q2" :priority 0}
                                                 ])]
      (let [job (tn/reserve-job env)
            job (update job :timed-out? deref)]
        (is (= (tu/adjust-tz env job) {:payload nil,
                                    :queue "q1",
                                    :name :unknown-job-name,
                                    :locked-by "sample-worker",
                                    :timed-out? false,
                                    :max-run-time 14400,
                                    :failed-at nil,
                                    :priority 1,
                                    :id 2,
                                    :run-at a-minute-ago
                                    :locked-at now})
            "[nil \"q1\"] for queues ignores jobs other than nil and q1 queues")
        (is (= (tu/job-table-data env) (update-in fixtures [1]
                                               (fn [job]
                                                 (assoc job
                                                        :locked-at now
                                                        :locked-by "sample-worker"))))
            "Job is locked")))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {}
                                                [{:run-at three-minutes-ago
                                                  :failed-at two-minutes-ago
                                                  :locked-at nil :locked-by nil
                                                  :queue nil :priority nil}
                                                 ])]
      (let [job (tn/reserve-job env)]
        (is (nil? job )
            "only available job has :failed-at set so no job found")
        (is (= (tu/job-table-data env) fixtures)
            "Job table is unchanged")))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {}
                                                [{:run-at three-minutes-ago
                                                  :failed-at nil
                                                  :locked-at three-minutes-ago
                                                  :locked-by nil
                                                  :queue nil :priority nil}
                                                 ])]
      (let [job (tn/reserve-job env)]
        (is (nil? job)
            "only available job has :locked-at set so no job found")
        (is (= (tu/job-table-data env) fixtures)
            "Job table is unchanged")))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {}
                                                [{:run-at a-minute-ago
                                                  :failed-at nil
                                                  :locked-at five-hours-ago
                                                  :locked-by "some-other-worker"
                                                  :queue nil :priority nil}
                                                 ])]
      (let [job (tn/reserve-job env)
            job (update job :timed-out? deref)]
        (is (= (tu/adjust-tz env job) {:payload nil,
                                    :queue nil,
                                    :name :unknown-job-name,
                                    :locked-by "sample-worker",
                                    :timed-out? false,
                                    :max-run-time 14400,
                                    :failed-at nil,
                                    :priority nil,
                                    :id 1,
                                    :run-at a-minute-ago
                                    :locked-at now})
            "Job is locked, but locked-at is longer than 4 hours, so has
              definitely timed out and is considered failed, so job is
              reserved")
        (is (= (tu/job-table-data env) (update-in fixtures [0]
                                               (fn [job]
                                                 (assoc job
                                                        :locked-at now
                                                        :locked-by "sample-worker"))))
            "Job is locked (again by this worker)"))
      )
    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {}
                                                [{:run-at nil
                                                  :failed-at nil
                                                  :locked-at nil
                                                  :locked-by "sample-worker"
                                                  :queue nil :priority nil}
                                                 ])]
      (let [job (tn/reserve-job env)
            job (update job :timed-out? deref)]
        (is (= (tu/adjust-tz env job) {:payload nil,
                                    :queue nil,
                                    :name :unknown-job-name,
                                    :locked-by "sample-worker",
                                    :timed-out? false,
                                    :max-run-time 14400,
                                    :failed-at nil,
                                    :priority nil,
                                    :id 1,
                                    :run-at nil
                                    :locked-at now})
            "Job is locked, but locked-by current worker. So reserving job
              irrespective of value of run-at.")
        (is (= (tu/job-table-data env) (update-in fixtures [0]
                                               (fn [job]
                                                 (assoc job
                                                        :locked-at now
                                                        :locked-by "sample-worker"))))
            "Job is locked (again by this worker)")))

    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {}
                                                [{:run-at a-minute-ago
                                                  :failed-at nil
                                                  :locked-at nil :locked-by nil
                                                  :handler "--- !ruby/struct:InvitationExpirationReminderJob
invitation_id: 882\nfoo: bar"
                                                  :queue nil :priority nil}])]
      (let [job (tn/reserve-job env)
            job (update job :timed-out? deref)]
        (is (= (tu/adjust-tz env job) {:payload {:invitation_id 882 :foo "bar"},
                                    :name :invitation-expiration-reminder-job,
                                    :handler "--- !ruby/struct:InvitationExpirationReminderJob
invitation_id: 882\nfoo: bar"
                                    :queue nil,
                                    :locked-by "sample-worker",
                                    :timed-out? false,
                                    :max-run-time 14400,
                                    :failed-at nil,
                                    :priority nil,
                                    :id 1,
                                    :run-at a-minute-ago,
                                    :locked-at now})
            "yaml is parsed, and payload and name keys set")
        (is (= (tu/job-table-data env) (update-in fixtures [0]
                                               (fn [job]
                                                 (assoc job
                                                        :locked-at now
                                                        :locked-by "sample-worker"))))
            "Job is locked")
        ))
    (let [{:keys [env worker fixtures]}
          (tu/prepare-for-test-merge-worker-config tu/defaults {}
                                                [{:run-at a-minute-ago
                                                  :failed-at nil
                                                  :locked-at nil :locked-by nil
                                                  :handler "foo: bar\nbaz boz"
                                                  :queue nil :priority nil}])]
      (is (thrown-with-msg? Exception #"Exception thrown parsing job yaml"
                            (tn/reserve-job env))
          "yaml parser throws error,"))))

(deftest reserve-and-run-one-job
  (let [fn-called (atom {})
        log-atom (atom [])
        failed-reserved-count (atom 0)
        some-exception (Exception. "some exception")
        yaml-exception (ex-info "yaml parse error" {:yaml-exception? true})
        env {:logger tu/test-logger
             :worker-id "some-worker"
             :worker-status (atom :running)
             :log-atom log-atom
             :on-reserve-fail :stop
             :max-failed-reserve-count 2
             :failed-reserved-count failed-reserved-count}]
    (with-redefs [dj-consumer.reserve-and-run/reserve-job (fn [{:keys [throw-reserve] :as env}]
                                                            (swap! fn-called assoc :reserve-job true)
                                                            (if throw-reserve (throw throw-reserve))
                                                            (:reserved-job env))
                  dj-consumer.reserve-and-run/run (fn [env job]
                                                    (swap! fn-called assoc :run job)
                                                    (:result job))
                  dj-consumer.reserve-and-run/failed (fn [env job]
                                                       (swap! fn-called assoc :failed job))
                  digicheck.util/exception-str (constantly "some exception string")]
      (is (= (tn/reserve-and-run-one-job (assoc env :reserved-job
                                                {:name "some-job"
                                                 :id 1
                                                 :result :job-result})) :job-result)
          "returned value is whatever the run fn returned")
      (is (= @fn-called {:reserve-job true,
                         :run {:name "some-job", :id 1 :result :job-result}})
          "reserve-job is called and then run with the reserved job")
      (is (= @log-atom []) "nothing logged")
      (is (= @failed-reserved-count 0))

      (reset! fn-called {})
      (reset! log-atom [])
      (is (= (tn/reserve-and-run-one-job (assoc env
                                                :throw-reserve yaml-exception
                                                :reserved-job {:name "some-job" :id 1})) :fail)
          "reserving job throws an yaml exception, our fn returns a :fail")
      (is (= @fn-called {:reserve-job true,
                         :failed {:exception yaml-exception
                                  :fail-reason "yaml parse error"}})
          "reserve-job is called, and then failed because yaml couldn't be parsed ")
      (is (= @log-atom [])
          "nothing is logged")
      (is (= @failed-reserved-count 0)
          "job is failed ")

      ;Handling random reserve exceptions:
      (reset! fn-called {})
      (reset! log-atom [])
      (is (= (tn/reserve-and-run-one-job (assoc env
                                                :throw-reserve some-exception
                                                :reserved-job {:name "some-job" :id 1})) :fail)
          "reserving job throws an exception, our fn returns a :fail")
      (is (= @fn-called {:reserve-job true,})
          "reserve-job is called only ")
      (is (= @(:worker-status env) :running)
          "worker status is now crashed")
      (is (= @log-atom
             [":error-[some-worker] Error while trying to reserve a job: \nsome exception string"])
          "reserve error is logged")
      (is (= @failed-reserved-count 1)
          "keeping track of number of reserve errors")

      ;One more reserve error:
      (reset! fn-called {})
      (reset! log-atom [])
      (is (= (tn/reserve-and-run-one-job (assoc env
                                                :throw-reserve some-exception
                                                :on-reserve-fail :stop
                                                :reserved-job {:name "some-job" :id 1})) :fail)
          "reserving job throws an exception, our fn returns a :fail")
      (is (= @fn-called {:reserve-job true,})
          "reserve-job is called")
      (is (= @(:worker-status env) :crashed)
          "worker status is now crashed")
      (is (= @log-atom
             [":error-[some-worker] Error while trying to reserve a job: \nsome exception string"
              ":error-[some-worker] Too many reserve failures. Worker stopped"])
          "reserve error is logged")
      (is (= @failed-reserved-count 2)
          "keeping track of number of reserve errors")

      (reset! fn-called {})
      (reset! log-atom [])
      (is (thrown-with-msg? Exception #"Too many reserve failures"
                            (tn/reserve-and-run-one-job (assoc env
                                                               :throw-reserve some-exception
                                                               :on-reserve-fail :throw
                                                               :reserved-job {:name "some-job" :id 1})) :fail)
          "reserving job throws an exception, too many have been thrown, as configured the whole thing throws"))))
