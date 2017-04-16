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
             [util :as u]
             [worker :as worker]]))

(def methods-called (atom {}))

(defmethod job/run :job1 [k job]
  (swap! methods-called assoc-in [k :run] job)
  )


(deftest worker-empty-job-table
  (let [{:keys [env fixtures worker <!!-status log-atom]}
        (tu/setup-worker-test
         {:worker-config  {:exit-on-complete? true}
          :job-records []})]
    (reset! methods-called {})
    (with-redefs [dj-consumer.util/exception-str
                  (fn [e] (str "Exception: " (.getMessage e)))]
      (worker/start worker)
      (loop []
        (let [status (<!!-status)]
          (if-not (contains? #{:stopped :timeout :done} status)
            (recur)
            (is (= status :done)
                "No jobs in table, exit-on-complete, so end status is done")))))
    (is (= (deref methods-called) {})
        "No methods called of course")
    (is (= (deref log-atom) [{:level :info, :text "[sample-worker] Starting"}
                             {:level :info,
                              :text "[sample-worker] No more jobs available. Exiting"}])
        "Start and exit logged")
    (is (empty? (tu/job-table-data env))
        "Job table is empty")))

(deftest worker-one-job-no-run-at
  (testing "One job in table, but all fields are default or nil, so also run-at"
    (let [{:keys [env fixtures worker <!!-status log-atom]}
          (tu/setup-worker-test
           {:worker-config  {:exit-on-complete? true}
            :job-records [{}]})] ;this'll insert a default job
      (with-redefs [dj-consumer.util/exception-str
                    (fn [e] (str "Exception: " (.getMessage e)))]
        (worker/start worker)
        (loop []
          (let [status (<!!-status)]
            (if-not (contains? #{:stopped :timeout :done} status)
              (recur)
              (is (= status :done)
                  "No jobs in table, exit-on-complete, so end status is done")))))
      (is (= (deref methods-called) {})
          "No methods called of course")
      (is (= (deref log-atom) [{:level :info, :text "[sample-worker] Starting"}
                               {:level :info,
                                :text "[sample-worker] No more jobs available. Exiting"}])
          "Start and exit logged")
      (is (= (tu/job-table-data env) [{:last-error nil,
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

;; (deftest worker-one-job-with-run-at-now
;;   (testing "One job in table, with run-at set to now"
;;     (let [{:keys [env fixtures worker <!!-status log-atom]}
;;           (setup-worker-test
;;            {:worker-config  {:exit-on-complete? true}
;;             :job-records [{}]})] ;this'll insert a default job
;;       (with-redefs [dj-consumer.util/exception-str
;;                     (fn [e] (str "Exception: " (.getMessage e)))]
;;         (worker/start worker)
;;         (loop []
;;           (let [status (<!!-status)]
;;             (if-not (contains? #{:stopped :timeout :done} status)
;;               (recur)
;;               (is (= status :done)
;;                   "No jobs in table, exit-on-complete, so end status is done")))))
;;       (is (= (deref methods-called) {})
;;           "No methods called of course")
;;       (is (= (deref log-atom) [{:level :info, :text "[sample-worker] Starting"}
;;                                {:level :info,
;;                                 :text "[sample-worker] No more jobs available. Exiting"}])
;;           "Start and exit logged")
;;       (is (= (tu/job-table-data env) [{:last-error nil,
;;                                        :queue nil,
;;                                        :locked-by nil,
;;                                        :attempts 0,
;;                                        :failed-at nil,
;;                                        :priority 0,
;;                                        :id 1,
;;                                        :run-at nil,
;;                                        :handler nil,
;;                                        :locked-at nil}])
;;           "Job table is unchanged"))))




;; (deftest mytest
;;   (testing "test one"
;;     (is (= 1 2)))
;;   (testing "test two"
;;     (is (= 2 3))))

;; (let [{:keys [env fixtures worker <!!-status log-atom]}
;;       (setup-worker-test
;;        {:worker-config  {:exit-on-complete? true
;;                          :poll-interval 1 ;sleep in seconds between batch jobs
;;                          ;; :max-attempts 25
;;                          ;; :max-failed-reserve-count 10
;;                          ;; :delete-failed-jobs? false
;;                          ;; :on-reserve-fail :stop ;or :throw
;;                          ;; :poll-batch-size 100 ;how many jobs to process for every poll
;;                          ;; :reschedule-at (fn [some-time attempts]
;;                          ;;                  (time/plus some-time (time/seconds (Math/pow attempts 4))))
;;                          ;; ;;Job selection:
;;                          ;; :min-priority nil
;;                          ;; :max-priority nil
;;                          ;; :queues nil ;nil is all queues, but nil is also a valid queue, eg [nil "q"]
;;                          }

;;         :job-records []})]

;;   (with-redefs [dj-consumer.util/exception-str (fn [e]
;;                                                  (str "Exception: " (.getMessage e)))]
;;     (worker/start worker)

;;     (loop []
;;       (let [status (<!!-status)]
;;         (if-not (contains? #{:stopped :timeout :done} status)
;;           (recur)
;;           (info "Worker status: " status)))))

;;   (pprint (deref methods-called))
;;   (pprint (deref log-atom))
;;   (pprint (tu/job-table-data env)))


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
