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

    [dj-consumer.job :as job]

in the namespace where you want to define the various lifecycle multimethods for
your job(s):

```
(defmethod job/run :some-job [_ job]
  (info "Doing job, sleeping 1000 ms")
  (Thread/sleep 1000)
  (if @(:timed-out? job)
     (pprint "Stopping job!!!!)
     (info  "Done the job " ))
  )
```

All hooks are passed the job as second parameter. This job is basically a map of
the delayed job database record with the yaml handler data parsed. The object's
data from the yaml handler is set to :payload and the name of the ruby
delayed job struct or of "object#method" to the :name key. The multimethod
dispatches on this :name key.

A job also comes with a :timed-out? key. This atom becomes true when job times
out. All lifecycle methods of a job get called regardless, take appropriate
action in each.If your job is sleeping or parked (waiting for channel input)
check the atom before continuing. Timed out jobs are rescheduled, so make sure
the job is idempotent or roll back any changes if needed.

All job multimethods are expected to be synchronous. If you need to do async
work, use core.async, or futures, delays and promises. If an error occurs throw
an exception, it will reschedule the job (up to max-attempts, defined for worker
and/or on job). If you don't want to reschedule job throw an ex-info with context
set to {:failed? true}

For lifecycle see dj-consumer.job

### Develop

    boot watch-and-install

Besides being able to start a repl in this repo, if you start your (boot) project with the
-c (checkout) flag like this:

    boot -c dj-consumer:0.1.0 <some boot task(s)>

any edits in dj-consumer source will compiled, installed and checked out again
in your project.

### TODO

- Passing in delayed job table name is assumed to be singular and keyword. Maybe
  pass in real table name.
- This lib uses some util fns and the db clauses namespace from bilby. Extract
  this stuff into libs and reuse. Whole of bilby should be collection of libs really.
- catch INT?
