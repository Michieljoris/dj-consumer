(ns dj-consumer.database.core-test
  (:require [dj-consumer.database.core :as tn]
            [clj-time.core :as time]
            [clojure.test :as t :refer [deftest is]]))

(deftest make-query-params
  (is (= (tn/make-query-params {} {:table :some-table
                                   :cols [:c1 :c2]
                                   :where [:and [[:c1 := 1]]]
                                   :limit {:count 1}
                                   :order-by [[:c1]]})
         {:table :some-table,
          :cols [:c1 :c2],
          :updates nil
          :where-clause ["where some_table.c1 = ?" 1],
          :limit-clause ["limit ?" 1],
          :order-by-clause ["order by some_table.c1"]}
         )
      "Proper clauses returned")
  (is (= (tn/make-query-params {} {:table :some-table})
         {:table :some-table,
          :cols nil
          :updates nil
          :where-clause nil,
          :limit-clause nil,
          :order-by-clause nil})
      "Nil for clauses when no proper params passed in"))

(deftest make-reserve-scope
  (is (=
       (tn/make-reserve-scope {:worker-id "some-worker" :table :some-job-table})
       {:table :some-job-table,
        :cols nil,
        :updates nil
        :where-clause ["where ( some_job_table.failed_at is NULL AND\n( ( some_job_table.run_at <= ? AND\n( some_job_table.locked_at is NULL OR some_job_table.locked_at < ? ) ) OR some_job_table.locked_by = ? ) )"
                       "run-at-before"
                       "locked-at-before"
                       "some-worker"],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_table.priority asc, some_job_table.run_at asc"],
        })
      "Basic where clause")
  (is (=
       (tn/make-reserve-scope {:worker-id "some-worker":table :some-job-table
                               :queues ["foo" "bar"]})
       {:table :some-job-table,
        :updates nil
        :cols nil,
        :where-clause
        ["where ( some_job_table.failed_at is NULL AND\n( ( some_job_table.run_at <= ? AND\n( some_job_table.locked_at is NULL OR some_job_table.locked_at < ? ) ) OR some_job_table.locked_by = ? ) AND some_job_table.queue in (?,?) )"
         "run-at-before"
         "locked-at-before"
         "some-worker"
         "foo"
         "bar"],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_table.priority asc, some_job_table.run_at asc"],
        })
      "queue clause" 
      )
  (is (=
       (tn/make-reserve-scope {:worker-id "some-worker" :table :some-job-table
                               :queues [nil]})
       {:table :some-job-table,
        :updates nil
        :cols nil,
        :where-clause
        ["where ( some_job_table.failed_at is NULL AND\n( ( some_job_table.run_at <= ? AND\n( some_job_table.locked_at is NULL OR some_job_table.locked_at < ? ) ) OR some_job_table.locked_by = ? ) AND some_job_table.queue is NULL )"
         "run-at-before"
         "locked-at-before"
         "some-worker"],
        :limit-clause ["limit ?" 1]
        :order-by-clause ["order by some_job_table.priority asc, some_job_table.run_at asc"],
        })
      "Add nil to queues to search for jobs with queue col set to null")
  (is (=
       (tn/make-reserve-scope {:worker-id "some-worker" :table :some-job-table
                               :queues [nil "foo"]})
       {:table :some-job-table,
        :updates nil
        :cols nil,
        :where-clause
        ["where ( some_job_table.failed_at is NULL AND\n( ( some_job_table.run_at <= ? AND\n( some_job_table.locked_at is NULL OR some_job_table.locked_at < ? ) ) OR some_job_table.locked_by = ? ) AND\n( some_job_table.queue in (?) OR some_job_table.queue is NULL ) )"
         "run-at-before"
         "locked-at-before"
         "some-worker"
         "foo"],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_table.priority asc, some_job_table.run_at asc"],
        })
      "Add nil and named queues to queues to also search for jobs with queue col set to null")
  (is (=
       (tn/make-reserve-scope {:worker-id "some-worker" :table :some-job-table
                               :min-priority 1 :max-priority 5})
       {:table :some-job-table,
        :cols nil,
        :updates nil
        :where-clause
        ["where ( some_job_table.failed_at is NULL AND\n( ( some_job_table.run_at <= ? AND\n( some_job_table.locked_at is NULL OR some_job_table.locked_at < ? ) ) OR some_job_table.locked_by = ? ) AND some_job_table.priority >= ? AND some_job_table.priority <= ? )"
         "run-at-before"
         "locked-at-before"
         "some-worker"
         1
         5],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_table.priority asc, some_job_table.run_at asc"],})
      "Set max and min priority"))

(deftest make-lock-job-scope
  (is (=
       (tn/make-lock-job-scope {:worker-id "some-worker" :max-run-time 3600}
                               (time/date-time 1970 11 5 15 0 0)
                               )
       {:where-clause [],
        :updates
        {:locked-by "some-worker" :locked-at "1970-11-05 15:00:00"},
        })
      "Locked job scope updates locked-by and locked-at"
      )
  (is (=
       (tn/make-lock-job-scope {:worker-id "some-worker" :max-run-time 3600
                                :reserve-scope (tn/make-reserve-scope {:table :some-job-table
                                                                       :worker-id "some-worker"})}
                               (time/date-time 1970 11 5 15 0 0))
         
       {:table :some-job-table,
        :cols nil,
        :updates
        {:locked-by "some-worker", :locked-at "1970-11-05 15:00:00"},
        :where-clause
        ["where ( some_job_table.failed_at is NULL AND\n( ( some_job_table.run_at <= ? AND\n( some_job_table.locked_at is NULL OR some_job_table.locked_at < ? ) ) OR some_job_table.locked_by = ? ) )"
         "1970-11-05 15:00:00"
         "1970-11-05 14:00:00"
         "some-worker"],
        :limit-clause ["limit ?" 1],
        :order-by-clause
        ["order by some_job_table.priority asc, some_job_table.run_at asc"]})
      "run_at and locked_by are set to now and now minus max-run-time"
      ))
