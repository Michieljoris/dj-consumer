(ns dj-consumer.util-test
  (:require
   [dj-consumer.util :as tn]
   [clojure.test :as t :refer [deftest is use-fixtures testing]]

   [clj-time.core :as time]
   [clj-time.coerce :as time-coerce]
   [clj-time.local :as time-local]

   ;; logging
   [taoensso.timbre :as timbre
    :refer (log  trace  debug  info  warn  error  fatal  report color-str
                 logf tracef debugf infof warnf errorf fatalf reportf
                 spy get-env log-env)]
   [clojure.pprint :refer (pprint)]
   [jansi-clj.core :refer :all]
   ))

(tn/extract-rails-obj-name y)
(deftest parse-ruby-yaml
  (is (= (tn/parse-ruby-yaml "--- !ruby/struct:InvitationExpirationReminderJob
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
      "Ignore ruby anotations, extract ruby object name and method and set on :name key")

  (is (thrown? Exception (tn/parse-ruby-yaml "foo: bar\nbaz foo"))
      "Incorrect yaml throws exception")
  )
