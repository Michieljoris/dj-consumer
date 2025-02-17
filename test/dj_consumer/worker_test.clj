(ns dj-consumer.worker-test
  (:require [clj-time.core :as time]
            [clojure
             [spec :as s]
             [test :as t :refer [deftest is testing]]]
            [clojure.core.async :as async :refer [chan put!]]
            [clojure.spec.test :as st]
            [dj-consumer
             [job :as job]
             [test-util :as tu]
             [worker :as worker]]
            [digicheck.common.util :as u]
            [taoensso.timbre :as timbre :refer [info]]
            [clojure.pprint :refer [pprint]]
            ))

(def methods-called (atom []))

;; (doseq [mm ['config 'run 'exception 'finally 'failed]] (ns-unmap *ns* mm))

(defmulti config job/dispatch)
(defmulti run job/dispatch)
(defmulti exception job/dispatch)
(defmulti finally job/dispatch)
(defmulti failed job/dispatch)

(defmethod config :default [job] (job/config job))
(defmethod run :default [job] (job/run job))
(defmethod exception :default [job] (job/exception job))
(defmethod finally :default [job] (job/finally job))
(defmethod failed :default [job] (job/failed job))

(defn invoke-hook
  "Calls hook (as keyword) on job, resolving hook to fn in this
  namespace"
  [hook job]
  (let [hook (resolve (symbol (str "dj-consumer.worker-test/" (name hook))))]
    (hook job)))

(defn log-hook [hook job]
  (swap! methods-called conj {:hook hook :job-name (:name job)}))

(def hook-to-bit {:config 0
                  :run 1
                  :exception 2
                  :finally 3
                  :failed 4})

(defn throw-on? [hook job]
  (let [bit (hook hook-to-bit)]
    (if (bit-test (:priority job) bit)
      (throw (ex-info (str "job is throwing in " (name hook)) {:hook hook})))))

;; (throw-on? :run {:priority 2})

(defn throw-flag [hooks]
  (reduce (fn [p hook]
            (bit-set p (hook hook-to-bit)))
          0 hooks))

;; (throw-flag [:config :run :exception :finally :failed])

(defmethod run :test-job1 [job]
  (log-hook :run job))

(defmethod run :test-job-throws [job]
  (log-hook :run job)
  (throw (ex-info "job is throwing" {:error :data})))

(defmethod run :test-job-sleeps-1-second [job]
  (log-hook :run job)
  (Thread/sleep 1000))

(defmethod config :test-job-with-all-hooks [job]
  (log-hook :config job)
  (throw-on? :config job))

(defmethod run :test-job-with-all-hooks [job]
  (log-hook :run job)
  (throw-on? :run job))

(defmethod exception :test-job-with-all-hooks [job]
  (log-hook :exception job)
  (throw-on? :exception job))

(defmethod finally :test-job-with-all-hooks [job]
  (log-hook :finally job)
  (throw-on? :finally job))

(defmethod failed :test-job-with-all-hooks [job]
  (log-hook :failed job)
  (throw-on? :failed job))

(deftest worker-empty-job-table
  (testing "No jobs in table"
    (let [{:keys [status fixtures log-atom job-table-data methods-called]}
          (tu/run-worker-once {} [] methods-called)]
      (is (= methods-called [])
          "No methods called of course")
      (is (= log-atom
             [":info-[sample-worker] Starting"
              ":info-[sample-worker] No more jobs available. Exiting"])
          "Start and exit logged")
      (is (empty? job-table-data)
          "Job table is empty"))))

(deftest worker-one-job-no-run-at
  (testing "One job in table, but all fields are default or nil, so also run-at"
    (let [{:keys [status fixtures log-atom job-table-data methods-called]}
          (tu/run-worker-once {} [{}] methods-called)] ;this'll insert a default job
      (is (= status :done)
          "No jobs in table, exit-on-complete, so end status is done")
      (is (= methods-called [])
          "No methods called of course")
      (is (= log-atom [":info-[sample-worker] Starting"
                       ":info-[sample-worker] No more jobs available. Exiting"])
          "Start and exit logged")
      (is (= job-table-data  [{:last-error nil,
                               :queue nil,
                               :locked-by nil,
                               :attempts 0,
                               :failed-at nil,
                               :priority 0,
                               :id 1,
                               :run-at nil,
                               :handler nil,
                               :locked-at nil}])
          "Job table is unchanged"))))

(deftest worker-one-job-with-run-at
  (testing "One job in table, with run-at set to now, but no job name (handler is nil)"
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime]
      (let [{:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [{:run-at tu/now :locked-by nil
                                     :locked-at nil :priority 0 :attempts 0 :handler nil
                                     :failed-at nil :queue nil}]
                                methods-called)]
        (is (= status :done)
            "No jobs in table, exit-on-complete, so end status is done")
        (is (= methods-called [])
            "No methods called of course")
        (is (= log-atom
               [":info-[sample-worker] Starting"
                ":info-[sample-worker] Job :unknown-job-name (id=1) RUNNING"
                ":error-[sample-worker] Job :unknown-job-name (id=1) FAILED to run. Job requested fail Last exception:\nException: Implementation is missing for job :unknown-job-name"
                ":error-[sample-worker] Job :unknown-job-name (id=1) MARKED failed because of job requested to be failed"
                ":info-[sample-worker] 1 jobs processed at 100.00 jobs per second. 1 failed."
                ":info-[sample-worker] No more jobs available. Exiting"])
            "Start and exit logged")
        (is (= job-table-data  [{:queue nil,
                                 :locked-by "sample-worker",
                                 :attempts 1,
                                 :failed-at tu/now,
                                 :priority 0,
                                 :id 1,
                                 :run-at tu/now,
                                 :handler nil,
                                 :locked-at tu/now}])
            "failed-at is set, as well as locked-by and locked-at, attempts is incremented")))))

(deftest worker-run-one-job-with-run-method
  (testing "One job in table, with run-at set to now, wth existing run method for job name as set in handler"
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime
                  dj-consumer.job/invoke-hook invoke-hook]
      (let [job1 {:run-at tu/now :locked-by nil
                  :locked-at nil :priority 0 :attempts 0
                  :handler (tu/make-handler-yaml {:job-name :test-job1 :payload {:foo 123}})
                  :failed-at nil :queue nil}
            {:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [job1] methods-called)]
        (is (= status :done)
            "One job in table, exit-on-complete, so end status is done")
        (is (= methods-called [{:hook :run :job-name :test-job1}])
            "Run method called of test-job1")
        (is (= log-atom [":info-[sample-worker] Starting"
                         ":info-[sample-worker] Job :test-job1 (id=1) RUNNING"
                         ":info-[sample-worker] Job :test-job1 (id=1) COMPLETED after 10ms"
                         ":info-[sample-worker] 1 jobs processed at 100.00 jobs per second. 0 failed."
                         ":info-[sample-worker] No more jobs available. Exiting"])
            "running and completing of job logged")
        (is (= job-table-data ())
            "Job ran successfully, removed from job table")))))

(deftest worker-run-multiple-jobs
  (testing "One job in table, with run-at set to now, wth existing run method for job name as set in handler"
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime
                  dj-consumer.job/invoke-hook invoke-hook]
      (let [job1 {:run-at tu/now :locked-by nil
                  :locked-at nil :priority 0 :attempts 0
                  :handler (tu/make-handler-yaml {:job-name :test-job1 :payload {:foo 123}})
                  :failed-at nil :queue nil}
            job2 {:run-at tu/now :handler (tu/make-handler-yaml {:job-name :test-job-throws :payload {}})}
            job3 {:run-at tu/now :handler (tu/make-handler-yaml {:job-name :non-existant-job :payload {}})}
            {:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [job1 job2 job3] methods-called)]
        (is (= status :done)
            "One job runs, one job throws and is rescheduled, one is non-existant and is failed")
        (is (= methods-called [{:hook :run, :job-name :test-job1}
                               {:hook :run, :job-name :test-job-throws}])
            "Run method called of test-job1")
        (is (= log-atom [":info-[sample-worker] Starting"
                         ":info-[sample-worker] Job :test-job1 (id=1) RUNNING"
                         ":info-[sample-worker] Job :test-job1 (id=1) COMPLETED after 10ms"
                         ":info-[sample-worker] Job :test-job-throws (id=2) RUNNING"
                         ":error-[sample-worker] Job :test-job-throws (id=2) FAILED to run.  Last exception:\nException: job is throwing"
                         ":info-[sample-worker] Job :test-job-throws (id=2) Rescheduled at Sun, 01 Jan 2017 13:00:01 +0000"
                         ":info-[sample-worker] Job :non-existant-job (id=3) RUNNING"
                         ":error-[sample-worker] Job :non-existant-job (id=3) FAILED to run. Job requested fail Last exception:\nException: Implementation is missing for job :non-existant-job"
                         ":error-[sample-worker] Job :non-existant-job (id=3) MARKED failed because of job requested to be failed"
                         ":info-[sample-worker] 3 jobs processed at 300.00 jobs per second. 2 failed."
                         ":info-[sample-worker] No more jobs available. Exiting"])
            "running and completing of jobs logged")
        (is (= job-table-data [{:queue nil,
                                :locked-by nil,
                                :attempts 1,
                                :failed-at nil,
                                :priority 0,
                                :id 2,
                                :run-at (time/plus tu/now (time/seconds 1)),
                                :handler "--- !ruby/struct:test-job-throws\n{}\n",
                                :locked-at nil}
                               {:queue nil,
                                :locked-by "sample-worker",
                                :attempts 1,
                                :failed-at tu/now ,
                                :priority 0,
                                :id 3,
                                :run-at tu/now,
                                :handler "--- !ruby/struct:non-existant-job\n{}\n",
                                :locked-at tu/now}])
            "Successful job is removed, failed job marked as such, throwing job is rescheduled")))))

(deftest worker-run-job-with-all-hooks-defined
  (testing "job with all hooks defined"
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime
                  dj-consumer.job/invoke-hook invoke-hook]
      (let [job {:run-at tu/now :locked-by nil
                 :locked-at nil :priority 0 :attempts 0 :failed-at nil
                 :handler (tu/make-handler-yaml {:job-name :test-job-with-all-hooks :payload {:foo 123}})}
            {:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [job] methods-called)]
        (is (= status :done)
            "Completed normally")
        (is (= methods-called [{:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :run, :job-name :test-job-with-all-hooks}
                               {:hook :finally, :job-name :test-job-with-all-hooks}])
            "Normal lifecycle hooks are called")
        (is (= log-atom [":info-[sample-worker] Starting"
                         ":info-[sample-worker] Job :test-job-with-all-hooks (id=1) RUNNING"
                         ":info-[sample-worker] Job :test-job-with-all-hooks (id=1) COMPLETED after 10ms"
                         ":info-[sample-worker] 1 jobs processed at 100.00 jobs per second. 0 failed."
                         ":info-[sample-worker] No more jobs available. Exiting"])
            "Running and completing logged")
        (is (= job-table-data [])
            "Successful job is removed")))))

(deftest worker-run-job-with-all-hooks-defined-config-throws
  (testing ""
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime
                  dj-consumer.job/invoke-hook invoke-hook]
      (let [job {:run-at tu/now :locked-by nil
                 :locked-at nil :priority (throw-flag [:config]) :attempts 0 :failed-at nil
                 :handler (tu/make-handler-yaml {:job-name :test-job-with-all-hooks :payload {:foo 123}})}
            {:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [job] methods-called)]
        (is (= status :crashed)
            "Job throws in config hook, reserve fails")
        (is (= methods-called [{:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :config, :job-name :test-job-with-all-hooks}])
            "10 failed config hooks")
        ;;Taking 12 because after test watcher sees worker-status change
        ;;to :crashed we continue the test. However a final log msg -might- be
        ;;added to the log atom because we have a race condition. But these don't affect
        ;;the test result.
        (is (= (take 12 log-atom)
               [":info-[sample-worker] Starting"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Error while trying to reserve a job: \nException: job is throwing in config"
                ":error-[sample-worker] Too many reserve failures. Worker stopped"
                ])
            "Reserve fails logged")
        (is (= job-table-data [{:locked-by "sample-worker",
                                :attempts 0,
                                :failed-at nil,
                                :priority 1,
                                :run-at tu/now ,
                                :handler "--- !ruby/struct:test-job-with-all-hooks\n{foo: 123}\n",
                                :locked-at tu/now,
                                :id 1}])
            "Worker crashed, so job is stil in locked state. If this worker is started up again, it will clear all its own locks and start this job again. If another worker starts up, it will pick up this job after a minimum of 4 hours have expired.")))))

(deftest worker-run-one-job-with-run-method-but-locked-already
  (testing "One job in table, with run-at set to now, wth existing run method
  for job name as set in handler, but job is locked already"
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime
                  dj-consumer.job/invoke-hook invoke-hook]
      (let [job1 {:run-at tu/now :locked-by "some-other-worker"
                  :locked-at tu/now :priority 0 :attempts 0
                  :handler (tu/make-handler-yaml {:job-name :test-job1 :payload {:foo 123}})
                  :failed-at nil :queue nil}
            {:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [job1] methods-called)]
        (is (= status :done)
            "One job in table, exit-on-complete, so end status is done")
        (is (= methods-called [])
            "Job is locked by another worker")
        (is (= log-atom [":info-[sample-worker] Starting"
                         ":info-[sample-worker] No more jobs available. Exiting"])
            "running logged")
        (is (= job-table-data fixtures)
            "job table has not changed")))))

(deftest worker-run-one-job-with-run-method-but-locked-five-hours-ago
  (testing "One job in table, with run-at set to now, wth existing run method
  for job name as set in handler, job is locked already, but more than 4 hours ago"
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime
                  dj-consumer.job/invoke-hook invoke-hook]
      (let [job1 {:run-at tu/now :locked-by "some-other-worker"
                  :locked-at tu/five-hours-ago :priority 0 :attempts 0
                  :handler (tu/make-handler-yaml {:job-name :test-job1 :payload {:foo 123}})
                  :failed-at nil :queue nil}
            {:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [job1] methods-called)]
        (is (= status :done)
            "One job in table, exit-on-complete, so end status is done")
        (is (= methods-called [{:hook :run :job-name :test-job1}])
            "Run method called of test-job1")
        (is (= log-atom [":info-[sample-worker] Starting"
                         ":info-[sample-worker] Job :test-job1 (id=1) RUNNING"
                         ":info-[sample-worker] Job :test-job1 (id=1) COMPLETED after 10ms"
                         ":info-[sample-worker] 1 jobs processed at 100.00 jobs per second. 0 failed."
                         ":info-[sample-worker] No more jobs available. Exiting"])
            "running and completing of job logged")
        (is (= job-table-data ())
            "Job ran successfully, removed from job table")
        "One job in table, exit-on-complete, so end status is done"))))

(deftest worker-run-job-with-all-hooks-defined-finally-throws
  (testing "Job that throws in finally fails and is rescheduled"
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime
                  dj-consumer.job/invoke-hook invoke-hook]
      (let [job {:run-at tu/now :locked-by nil
                 :locked-at nil :priority (throw-flag [:finally]) :attempts 0 :failed-at nil
                 :handler (tu/make-handler-yaml {:job-name :test-job-with-all-hooks :payload {:foo 123}})}
            {:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [job] methods-called)]
        (is (= status :done)
            "Job failed")
        (is (= methods-called [{:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :run, :job-name :test-job-with-all-hooks}
                               {:hook :finally, :job-name :test-job-with-all-hooks}])
            "All 3 hooks called")
        (is (= log-atom [":info-[sample-worker] Starting"
                         ":info-[sample-worker] Job :test-job-with-all-hooks (id=1) RUNNING"
                         ":error-[sample-worker] Job :test-job-with-all-hooks (id=1) FAILED to run.  Last exception:\nException: job is throwing in finally"
                         ":info-[sample-worker] Job :test-job-with-all-hooks (id=1) Rescheduled at Sun, 01 Jan 2017 13:00:01 +0000"
                         ":info-[sample-worker] 1 jobs processed at 100.00 jobs per second. 1 failed."
                         ":info-[sample-worker] No more jobs available. Exiting"])
            "Job fails and is rescheduled")
        (is (= job-table-data [{:locked-by nil,
                                :attempts 1,
                                :failed-at nil,
                                :priority 8,
                                :run-at (time/plus tu/now (time/seconds 1)),
                                :handler "--- !ruby/struct:test-job-with-all-hooks\n{foo: 123}\n",
                                :locked-at nil,
                                :id 1}])
            "Worker crashed, so job is stil in locked state. If this worker is started up again, it will clear all its own locks and start this job again. If another worker starts up, it will pick up this job after a minimum of 4 hours have expired.")))))

(deftest worker-run-job-with-all-hooks-defined-run-and-exception-throw
  (testing "Job that throws in exception (again) still just fails and is rescheduled"
    (with-redefs [digicheck.common.util/now (constantly tu/u-now)
                  digicheck.common.util/runtime  tu/mock-runtime
                  dj-consumer.job/invoke-hook invoke-hook]
      (let [job {:run-at tu/now :locked-by nil
                 :locked-at nil :priority (throw-flag [:run :exception]) :attempts 0 :failed-at nil
                 :handler (tu/make-handler-yaml {:job-name :test-job-with-all-hooks :payload {:foo 123}})}
            {:keys [status fixtures log-atom job-table-data methods-called]}
            (tu/run-worker-once {} [job] methods-called)]
        (is (= status :done)
            "Job's failed and is rescheduled")
        (is (= methods-called [{:hook :config, :job-name :test-job-with-all-hooks}
                               {:hook :run, :job-name :test-job-with-all-hooks}
                               {:hook :exception, :job-name :test-job-with-all-hooks}
                               {:hook :finally, :job-name :test-job-with-all-hooks}])
            "4 hooks called")
        (is (= log-atom [":info-[sample-worker] Starting"
                         ":info-[sample-worker] Job :test-job-with-all-hooks (id=1) RUNNING"
                         ":error-[sample-worker] Job :test-job-with-all-hooks (id=1) FAILED to run.  Last exception:\nException: job is throwing in exception"
                         ":info-[sample-worker] Job :test-job-with-all-hooks (id=1) Rescheduled at Sun, 01 Jan 2017 13:00:01 +0000"
                         ":info-[sample-worker] 1 jobs processed at 100.00 jobs per second. 1 failed."
                         ":info-[sample-worker] No more jobs available. Exiting"])
            "Job fails and is rescheduled")
        (is (= job-table-data [{:locked-by nil,
                                :attempts 1,
                                :failed-at nil,
                                :priority 6,
                                :run-at (time/plus tu/now (time/seconds 1)),
                                :handler "--- !ruby/struct:test-job-with-all-hooks\n{foo: 123}\n",
                                :locked-at nil,
                                :id 1}])
            "rescheduled")))))
