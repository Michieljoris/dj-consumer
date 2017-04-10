#  def max_run_time
#         return unless payload_object.respond_to?(:max_run_time)
#         return unless (run_time = payload_object.max_run_time)

#         if run_time > Delayed::Worker.max_run_time
#           Delayed::Worker.max_run_time
#         else
#           run_time
#         end
# end

 # When a worker is exiting, make sure we don't have any locked jobs.
 def self.clear_locks!(worker_name)
          where(locked_by: worker_name).update_all(locked_by: nil, locked_at: nil)
end

# Unlock this job (note: not saved to DB)
# def job.unlock
#   self.locked_at    = nil
#   self.locked_by    = nil
# end

# def job.fail!
#   update_attributes(:failed_at => self.class.db_time_now)
# end
;
# def failed(job)
#   self.class.lifecycle.run_callbacks(:failure, self, job) do
#     begin
#       job.hook(:failure)
#     rescue => error
#       say "Error when running failure callback: #{error}", 'error'
#       say error.backtrace.join("\n"), 'error'
#     ensure
#       job.destroy_failed_jobs? ? job.destroy : job.fail!
#     end
#   end
# end

# Reschedule the job in the future (when a job fails).
# Uses an exponential scale depending on the number of failed attempts.
# def reschedule(job, time = nil)
#   if (job.attempts += 1) < max_attempts(job)
#     time ||= job.reschedule_at
#     job.run_at = time
#     job.unlock
#     job.save!
#   else
#     job_say job, "REMOVED permanently because of #{job.attempts} consecutive failures", 'error'
#     failed(job)
#   end
# end

# def handle_failed_job(job, error)
#   job.error = error
#   job_say job, "FAILED (#{job.attempts} prior attempts) with #{error.class.name}: #{error.message}", 'error'
#   reschedule(job)
# end

# def invoke_job
#   Delayed::Worker.lifecycle.run_callbacks(:invoke_job, self) do
#     begin
#       hook :before
#       payload_object.perform
#       hook :success
#     rescue Exception => e # rubocop:disable RescueException
#       hook :error, e
#       raise e
#     ensure
#       hook :after
#     end
#   end
# end

# def run(job)
#   job_say job, 'RUNNING'
#   runtime = Benchmark.realtime do
#     Timeout.timeout(max_run_time(job).to_i, WorkerTimeout) { job.invoke_job }
#     job.destroy
#   end
#   job_say job, format('COMPLETED after %.4f', runtime)
#   return true # did work
# rescue DeserializationError => error
#   job_say job, "FAILED permanently with #{error.class.name}: #{error.message}", 'error'

#   job.error = error
#   failed(job)
# rescue Exception => error # rubocop:disable RescueException
#   self.class.lifecycle.run_callbacks(:error, self, job) { handle_failed_job(job, error) }
#   job.error = error
#   job_say job, "FAILED (#{job.attempts} prior attempts) with #{error.class.name}: #{error.message}", 'error'
#   reschedule(job)
#   return false # work failed
# end

# def reserve_job
#   job = Delayed::Job.reserve(self)
#   @failed_reserve_count = 0
#   job
# rescue ::Exception => error # rubocop:disable RescueException
#   say "Error while reserving job: #{error}"
#   Delayed::Job.recover_from(error)
#   @failed_reserve_count += 1
#   raise FatalBackendError if @failed_reserve_count >= 10
#   nil
# end

# def reserve_and_run_one_job
#   job = reserve_job
#   self.class.lifecycle.run_callbacks(:perform, self, job) { run(job) } if job
# end

# Do num jobs and return stats on success/failure.
# Exit early if interrupted.
def work_off(num = 100)
  success = 0
  failure = 0

  num.times do
    case reserve_and_run_one_job
    when true
      success += 1
    when false
      failure += 1
    else
      break # leave if no work could be done
    end
    break if stop? # leave if we're exiting
  end

  [success, failure]
end


def start
    loop do
      self.class.lifecycle.run_callbacks(:loop, self) do
        @realtime = Benchmark.realtime do
          @result = work_off
        end
      end

      count = @result[0] + @result[1]

      if count.zero?
        if self.class.exit_on_complete
          say 'No more jobs available. Exiting'
          break
        elsif !stop?
          sleep(self.class.sleep_delay)
        end
      else
        say format("#{count} jobs processed at %.4f j/s, %d failed", count / @realtime, @result.last)
      end

      break if stop?
    end
end
