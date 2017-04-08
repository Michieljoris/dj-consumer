# dj-consumer

### Install

Run

    boot install

in this repo.

Add to your dependencies:

    [dj-consumer "0.1.0"]

Replace the version with the version in build.boot

Require:

    [dj-consumer.worker :as w]

### Use

Make a worker:

```
  (let [worker (w/make-worker{:worker-id :sample-worker
                            :sql-log? true
                            :db-config {:user "root"
                                        :password ""
                                        :url "//localhost:3306/"
                                        :db-name "chin_minimal"
                                        }})]
    (w/start worker)
    )
```

See for more options dj-consumer.worker.

Define a job by requiring

    [dj-consumer.job]

in the namespace where you want to define the various lifecycle multimethods for
your job(s):

```
(defmethod job/run :some-job [_ job _ _]
  (info "Doing job, sleeping 1000 ms")
  (Thread/sleep 1000)
  (info "Woke up!. Done the job " )
  ;; (put! c :done)
  )
```

For lifecycle see dj-consumer.job

### Develop

    boot watch-and-install

Besides being able to start a repl in this repo, if you start your (boot) project with the
-c (checkout) flag like this:

    boot -c dj-consumer:0.1.0 <some boot task(s)>

any edits in dj-consumer source will compiled, installed and checked out again
in your project.
