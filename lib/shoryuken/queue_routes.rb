# frozen_string_literal: true

module Shoryuken
  # Compiles boot-time routing configuration without contacting SQS.
  class QueueRoutes
    def initialize(options, named_clients)
      @clients = normalize_map(named_clients, 'sqs_named_clients')
      @profiles = normalize_map(options.fetch(:sqs_clients, {}), 'sqs_clients')
      @routes = normalize_map(options.fetch(:sqs_queue_routes, {}), 'sqs_queue_routes')

      @clients.each do |name, client|
        invalid!("sqs_named_clients.#{name}", 'must be an Aws::SQS::Client') unless client.is_a?(Aws::SQS::Client)
      end
      (@profiles.keys & @clients.keys).each { |name| invalid!("sqs_clients.#{name}", 'is already registered in Ruby') }
      @profiles.each do |name, definition|
        path = "sqs_clients.#{name}"
        @profiles[name] = validate_fields(definition, path, %w[profile region])
      end
      @routes.each do |name, definition|
        path = "sqs_queue_routes.#{name}"
        route = validate_fields(definition, path, %w[client queue])
        client = route.fetch('client')
        invalid!("#{path}.client", 'must reference a named SQS client') unless @clients[client] || @profiles[client]
        @routes[name] = route
      end
    end

    # Called under the Client queue-cache mutex.
    def resolve(identifier)
      return unless (route = @routes[identifier])

      name = route.fetch('client')
      [@clients[name] ||= build_client(name), route.fetch('queue')]
    end

    private

    def build_client(name)
      definition = @profiles.fetch(name)
      credentials = Aws::SharedCredentials.new(profile_name: definition.fetch('profile'))
      return Aws::SQS::Client.new(region: definition.fetch('region'), credentials: credentials) if credentials.set?

      invalid_profile!(name)
    rescue Aws::Errors::NoSuchProfileError
      invalid_profile!(name)
    end

    def invalid_profile!(name)
      invalid!("sqs_clients.#{name}.profile", 'must resolve to credentials in the AWS shared credentials file')
    end

    def validate_fields(definition, path, fields)
      values = normalize_map(definition, path)
      invalid!(path, "only accepts #{fields.join(', ')}") unless (values.keys - fields).empty?

      fields.each do |field|
        value = values[field]
        if field == 'client' && value.is_a?(Symbol)
          values[field] = value.to_s
        elsif !value.is_a?(String) || value.strip.empty?
          invalid!("#{path}.#{field}", 'must be a non-empty string')
        else
          values[field] = value.dup.freeze
        end
      end
      values
    end

    def normalize_map(value, path)
      invalid!(path, 'must be a mapping') unless value.is_a?(Hash)

      value.each_with_object({}) do |(key, entry), result|
        unless (key.is_a?(String) || key.is_a?(Symbol)) && !key.to_s.strip.empty?
          invalid!(path, 'must use non-empty string keys')
        end

        name = key.to_s
        invalid!("#{path}.#{name}", 'is defined more than once') if result.key?(name)
        result[name] = entry
      end
    end

    def invalid!(path, message)
      raise Errors::InvalidConfigurationError, "#{path} #{message}"
    end
  end
end
