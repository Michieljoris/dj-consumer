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
(defmethod job/run :some-job [_ job]
  (info "Doing job, sleeping 1000 ms")
  (Thread/sleep 1000)
  (if @(:stop? job)
     (pprint "Stopping job!!!!)
     (info  "Done the job " ))
  )
```

A job comes with a stop? key. This atom is true if job times out or worker is
stopped. All lifecycle methods of a job get called regardless, take appropriate
action in each.If your job is sleeping or parked (waiting for channel input)
check the atom before continuing. Timed out jobs are rescheduled, so make sure
the job is idempotent or roll back any changes if needed.

All job multimethods are expected to be synchronous. If you need to do async
work, use core.async, or futures, delays and promises. If an error occurs throw
an exception, it will reschedule the job (up to max-attempts, defined for worker
and/or on job). If you don't want to reschedule job throw an ex-info with data
set to {:fail! true}

For lifecycle see dj-consumer.job

### Develop

    boot watch-and-install

Besides being able to start a repl in this repo, if you start your (boot) project with the
-c (checkout) flag like this:

    boot -c dj-consumer:0.1.0 <some boot task(s)>

any edits in dj-consumer source will compiled, installed and checked out again
in your project.
