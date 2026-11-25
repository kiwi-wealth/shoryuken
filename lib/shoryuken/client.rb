# frozen_string_literal: true

module Shoryuken
  # Client class for interacting with SQS queues.
  # Provides a simple interface for accessing and managing queue instances.
  class Client
    # Cached queue instances by name (Hash{String => Shoryuken::Queue}).
    @@queues = {}

    # Guards the queue cache. queues is called concurrently from the dispatch
    # thread, processor-completion threads and worker threads, and building a
    # Shoryuken::Queue makes SQS API calls, so an unsynchronized `||=` would let
    # several callers build the same queue (and corrupt the hash on JRuby).
    @@queues_mutex = Mutex.new

    class << self
      # Returns a Queue instance for the given queue name
      #
      # @param name [String, Symbol] the name of the queue
      # @return [Shoryuken::Queue] the queue instance
      def queues(name)
        identifier = name.to_s
        @@queues_mutex.synchronize do
          if (@strict_queue_routes || Shoryuken.options[:sqs_strict_queue_routes] == true) &&
             !@queue_routes&.include?(identifier)
            raise Errors::InvalidConfigurationError, "No SQS queue route configured for #{identifier.inspect}"
          end

          @@queues[identifier] ||= begin
            route = @queue_routes&.resolve(identifier)
            client, target = route || [sqs, identifier]
            Shoryuken::Queue.new(client, target)
          end
        end
      end

      # Enables strict routing before application code can enqueue.
      def enable_strict_queue_routes!
        @@queues_mutex.synchronize do
          Shoryuken.options[:sqs_strict_queue_routes] = true
          @strict_queue_routes = true
          @queue_routes = nil
          @@queues = {}
        end
      end

      # Installs routes at boot, before starting consumers or producers.
      def configure_queue_routes(options = Shoryuken.options)
        strict = options.fetch(:sqs_strict_queue_routes, false)
        unless [true, false].include?(strict)
          raise Errors::InvalidConfigurationError, 'sqs_strict_queue_routes must be a boolean'
        end

        routes = QueueRoutes.new(options, Shoryuken.sqs_named_clients)
        @@queues_mutex.synchronize do
          @queue_routes = routes
          @strict_queue_routes = strict
          @@queues = {}
        end
      end

      # Returns the current SQS client
      #
      # @return [Aws::SQS::Client] the SQS client
      def sqs
        Shoryuken.sqs_client
      end

      # Sets a new SQS client and clears the queue cache
      #
      # @param sqs [Aws::SQS::Client] the new SQS client
      # @return [Aws::SQS::Client] the SQS client
      def sqs=(sqs)
        # Since the @@queues values (Shoryuken::Queue objects) are built referencing @@sqs, if it changes, we need to
        #   re-build them on subsequent calls to `.queues(name)`. Swap the client and reset the cache inside the same
        #   critical section: otherwise a concurrent `queues` call could slip between the reset and the assignment,
        #   read the old client, and cache a queue built with it that survives the replacement.
        @@queues_mutex.synchronize do
          Shoryuken.sqs_client = sqs
          @@queues = {}
        end
      end
    end
  end
end
