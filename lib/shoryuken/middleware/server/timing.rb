# frozen_string_literal: true

module Shoryuken
  module Middleware
    module Server
      # Middleware that logs timing information for message processing. Records start time, completion time, and warns if
      # processing exceeds the queue's visibility timeout.
      class Timing
        include Util

        # Processes a message while logging timing information
        #
        # @param _worker [Object] the worker instance (unused)
        # @param queue [String] the queue name
        # @param sqs_msg [Shoryuken::Message, Array<Shoryuken::Message>] the message or batch
        # @param _body [Object] the parsed message body (unused)
        # @yield continues to the next middleware in the chain
        # @return [void]
        def call(_worker, queue, sqs_msg, _body)
          started_at = Time.now

          logger.info { "started at #{started_at}" }

          yield

          total_time = elapsed(started_at)

          source_queue = source_queue_for(sqs_msg) || Shoryuken::Client.queues(queue)
          if (total_time / 1000.0) > (timeout = source_queue.visibility_timeout)
            logger.warn { "exceeded the queue visibility timeout by #{total_time - (timeout * 1000)} ms" }
          end

          logger.info { "completed in: #{total_time} ms" }
        rescue
          logger.info { "failed in: #{elapsed(started_at)} ms" }
          raise
        end
      end
    end
  end
end
