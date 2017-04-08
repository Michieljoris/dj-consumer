(ns dj-consumer.database-test
  (:require [dj-consumer.database :as tn2]
            [clj-time.core]
            [clojure.test :as t :refer [deftest is]]))

(deftest make-query-params
  (is (= (tn2/make-query-params {:table :some-table
                                :cols [:c1 :c2]
                                :where [:and [[:c1 := 1]]]
                                :limit {:count 1}
                                :order-by [[:c1]]})
         {:table :some-table,
          :cols [:c1 :c2],
          :updates nil
          :where-clause ["where some_tables.c1 = ?" 1],
          :limit-clause ["limit ?" 1],
          :order-by-clause ["order by some_tables.c1"]}
         )
      "Proper clauses returned")
  (is (= (tn2/make-query-params {:table :some-table})
         {:table :some-table,
          :cols nil
          :updates nil
          :where-clause nil,
          :limit-clause nil,
          :order-by-clause nil})
      "Nil for clauses when no proper params passed in"))

(deftest make-reserve-scope
  (is (=
       (tn2/make-reserve-scope {:worker-id "some-worker" :table :some-job-table})
       {:table :some-job-table,
        :cols nil,
        :updates nil
        :where-clause ["where ( some_job_tables.failed_at is NULL AND\n( ( some_job_tables.run_at <= ? AND\n( some_job_tables.locked_at is NULL OR some_job_tables.locked_at < ? ) ) OR some_job_tables.locked_by = ? ) )"
                       "run-at-before"
                       "locked-at-before"
                       "some-worker"],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_tables.priority asc, some_job_tables.run_at asc"],
        })
      "Basic where clause")
  (is (=
       (tn2/make-reserve-scope {:worker-id "some-worker":table :some-job-table
                               :queues ["foo" "bar"]})
       {:table :some-job-table,
        :updates nil
        :cols nil,
        :where-clause
        ["where ( some_job_tables.failed_at is NULL AND\n( ( some_job_tables.run_at <= ? AND\n( some_job_tables.locked_at is NULL OR some_job_tables.locked_at < ? ) ) OR some_job_tables.locked_by = ? ) AND some_job_tables.queue in (?,?) )"
         "run-at-before"
         "locked-at-before"
         "some-worker"
         "foo"
         "bar"],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_tables.priority asc, some_job_tables.run_at asc"],
        })
      "queue clause" 
      )
  (is (=
       (tn2/make-reserve-scope {:worker-id "some-worker" :table :some-job-table
                               :queues [nil]})
       {:table :some-job-table,
        :updates nil
        :cols nil,
        :where-clause
        ["where ( some_job_tables.failed_at is NULL AND\n( ( some_job_tables.run_at <= ? AND\n( some_job_tables.locked_at is NULL OR some_job_tables.locked_at < ? ) ) OR some_job_tables.locked_by = ? ) AND some_job_tables.queue is NULL )"
         "run-at-before"
         "locked-at-before"
         "some-worker"],
        :limit-clause ["limit ?" 1]
        :order-by-clause ["order by some_job_tables.priority asc, some_job_tables.run_at asc"],
        })
      "Add nil to queues to search for jobs with queue col set to null")
  (is (=
       (tn2/make-reserve-scope {:worker-id "some-worker" :table :some-job-table
                               :queues [nil "foo"]})
       {:table :some-job-table,
        :updates nil
        :cols nil,
        :where-clause
        ["where ( some_job_tables.failed_at is NULL AND\n( ( some_job_tables.run_at <= ? AND\n( some_job_tables.locked_at is NULL OR some_job_tables.locked_at < ? ) ) OR some_job_tables.locked_by = ? ) AND\n( some_job_tables.queue in (?) OR some_job_tables.queue is NULL ) )"
         "run-at-before"
         "locked-at-before"
         "some-worker"
         "foo"],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_tables.priority asc, some_job_tables.run_at asc"],
        })
      "Add nil and named queues to queues to also search for jobs with queue col set to null")
  (is (=
       (tn2/make-reserve-scope {:worker-id "some-worker" :table :some-job-table
                               :min-priority 1 :max-priority 5})
       {:table :some-job-table,
        :cols nil,
        :updates nil
        :where-clause
        ["where ( some_job_tables.failed_at is NULL AND\n( ( some_job_tables.run_at <= ? AND\n( some_job_tables.locked_at is NULL OR some_job_tables.locked_at < ? ) ) OR some_job_tables.locked_by = ? ) AND some_job_tables.priority >= ? AND some_job_tables.priority <= ? )"
         "run-at-before"
         "locked-at-before"
         "some-worker"
         1
         5],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_tables.priority asc, some_job_tables.run_at asc"],})
      "Set max and min priority"))

(deftest make-lock-job-scope
  (is (=
       (tn2/make-lock-job-scope {:worker-id :some-worker :max-run-time 3600}
                               (clj-time.core/date-time 1970 11 5 15 0 0)
                               )
       {:where-clause [],
        :updates
        {:locked-by ":some-worker" :locked-at "1970-11-05 15:00:00"},
        })
      "Locked job scope updates locked-by and locked-at"
      )
  (is (=
       (tn2/make-lock-job-scope {:worker-id :some-worker :max-run-time 3600
                                :reserve-scope (tn2/make-reserve-scope {:table :some-job-table})}
                               (clj-time.core/date-time 1970 11 5 15 0 0))
         
       {:table :some-job-table,
        :cols nil,
        :where-clause
        ["where ( some_job_tables.failed_at is NULL AND\n( ( some_job_tables.run_at <= ? AND\n( some_job_tables.locked_at is NULL OR some_job_tables.locked_at < ? ) ) OR some_job_tables.locked_by = ? ) )"
         "1970-11-05 15:00:00"
         "1970-11-05 14:00:00"
         ""],
        :limit-clause ["limit ?" 1],
        :order-by-clause ["order by some_job_tables.priority asc, some_job_tables.run_at asc"],
        :updates
        {:locked-by ":some-worker" :locked-at "1970-11-05 15:00:00"},}
       )
      "run_at and locked_by are set to now and now minus max-run-time"
      ))








































