# frozen_string_literal: true

require 'tempfile'
require 'active_job/queue_adapters/shoryuken_adapter'

RSpec.describe Shoryuken::QueueRoutes do
  let(:east_url) { 'https://sqs.us-east-1.amazonaws.com/111111111111/orders.fifo' }
  let(:west_url) { 'https://sqs.us-west-2.amazonaws.com/222222222222/orders.fifo' }
  let(:east) { Aws::SQS::Client.new(region: 'us-east-1', stub_responses: true) }
  let(:west) { Aws::SQS::Client.new(region: 'us-west-2', stub_responses: true) }
  let(:default) { Aws::SQS::Client.new(region: 'us-east-1', stub_responses: true) }
  let(:routes) do
    { 'orders-east' => { client: :east, queue: east_url }, 'orders-west' => { client: :west, queue: west_url } }
  end
  let(:fifo_attributes) { { 'FifoQueue' => 'true', 'RedrivePolicy' => '{"maxReceiveCount":2}' } }
  let(:auto_deleting_worker) { Class.new { def self.auto_delete? = true }.new }

  before do
    Shoryuken::Client.sqs = default
    Shoryuken.sqs_named_clients.merge!(east: east, west: west)
  end

  after do
    Shoryuken.sqs_named_clients.clear
    Shoryuken.options.delete(:sqs_queue_routes)
    Shoryuken.options.delete(:sqs_clients)
    Shoryuken.options.delete(:sqs_strict_queue_routes)
    Shoryuken::Client.configure_queue_routes({})
  end

  %i[load check].each do |method|
    it "resolves clients registered in a --require file during #{method}" do
      Shoryuken.options[:sqs_queue_routes] = { 'orders-required' => { client: 'required', queue: east_url } }
      Shoryuken.options[:queues] = ['orders-required']
      Tempfile.create(['shoryuken-worker', '.rb']) do |file|
        file.write(<<~RUBY)
          Shoryuken.sqs_named_clients[:required] = Shoryuken.sqs_named_clients.fetch(:east)
          Shoryuken.register_worker('orders-required', TestWorker)
        RUBY
        file.flush

        result = Shoryuken::EnvironmentLoader.setup_options(require: file.path).public_send(method)
        expect(result).to eq([]) if method == :check
      ensure
        Shoryuken.options.delete(:require)
      end

      expect(east.api_requests.map { |request| request[:params][:queue_url] }).to eq([east_url])
      expect(default.api_requests).to be_empty
    end
  end

  it 'uses separate clients for startup validation and operations on queues with the same name' do
    Shoryuken.options[:sqs_queue_routes] = routes
    Shoryuken.options[:queues] = routes.keys
    Shoryuken.options[:groups] = nil
    Class.new(TestWorker).shoryuken_options queue: 'orders-east', batch: true
    Shoryuken.register_worker('orders-west', TestWorker)
    [east, west].each { |client| client.stub_responses(:get_queue_attributes, attributes: fifo_attributes) }
    expect(Shoryuken::EnvironmentLoader.new({}).check).to eq([])

    fetcher = Shoryuken::Fetcher.new('default')
    targets = [[east, east_url, 'orders-east', 10], [west, west_url, 'orders-west', 1]]
    targets.each do |client, url, key, limit|
      response = { receipt_handle: key, attributes: { 'ApproximateReceiveCount' => '2' } }
      client.stub_responses(:receive_message, messages: [response])
      queue = Shoryuken::Polling::QueueConfiguration.new(key, {})
      message = fetcher.fetch(queue, 10).first

      expect(message.queue_name).to eq('orders.fifo')
      expect(message.last_receive?).to be(true)
      message.change_visibility(visibility_timeout: 60)
      Shoryuken::Middleware::Server::AutoDelete.new.call(auto_deleting_worker, key, [message], nil) {}

      expect(client.api_requests.map { |request| request[:params][:queue_url] }.uniq).to eq([url])
      receive = client.api_requests.find { |request| request[:operation_name] == :receive_message }
      expect(receive[:params][:max_number_of_messages]).to eq(limit)
      delete = client.api_requests.find { |request| request[:operation_name] == :delete_message_batch }
      expect(delete[:params][:entries]).to eq([{ id: '0', receipt_handle: key }])
    end
    expect(default.api_requests).to be_empty
  end

  it 'loads producer routing from YAML and routes native and batched Active Job sends' do
    yaml_routes = routes.transform_values { |route| route.transform_keys(&:to_s).transform_values(&:to_s) }
    Tempfile.create('shoryuken-queue-routes') do |file|
      file.write(YAML.dump('sqs_queue_routes' => yaml_routes, 'queues' => routes.keys))
      file.flush
      Shoryuken::EnvironmentLoader.load_queue_routes(config_file: file.path)
    end
    expect(Shoryuken.ungrouped_queues).to be_empty

    TestWorker.perform_async('native job', queue: 'orders-east')
    job_class = Class.new(ActiveJob::Base)
    stub_const('RoutedTestJob', job_class)
    jobs = routes.keys.map { |key| job_class.new.tap { |job| job.queue_name = key } }
    ActiveJob::QueueAdapters::ShoryukenAdapter.new.enqueue_all(jobs)

    expect(east.api_requests.filter_map { |r| r[:params][:queue_url] }.uniq).to eq([east_url])
    expect(west.api_requests.filter_map { |r| r[:params][:queue_url] }.uniq).to eq([west_url])
    expect(east.api_requests.map { |r| r[:operation_name] }).to include(:send_message, :send_message_batch)
    expect(west.api_requests.map { |r| r[:operation_name] }).to include(:send_message_batch)
    expect(default.api_requests).to be_empty
  end

  it 'only uses the default when no route exists, not when a mapped queue denies access' do
    Shoryuken::Client.configure_queue_routes(sqs_queue_routes: routes)
    east.stub_responses(:get_queue_attributes, 'AccessDenied')
    default.stub_responses(:get_queue_url, queue_url: east_url)

    expect { Shoryuken::Client.queues('orders-east') }.to raise_error(Aws::SQS::Errors::AccessDenied)
    expect(default.api_requests).to be_empty
    expect(Shoryuken::Client.queues('orders').client).to eq(default)
  end

  it 'rejects unknown queues in strict mode but permits explicit default-client routes' do
    config = {
      sqs_strict_queue_routes: true,
      sqs_queue_routes: routes.merge('local' => { client: nil, queue: east_url })
    }
    Shoryuken::Client.configure_queue_routes(config)

    expect(Shoryuken::Client.queues('local').client).to eq(default)
    expect(Shoryuken::Client.queues('orders-east').client).to eq(east)
    expect { TestWorker.perform_async('job', queue: 'orders-typo') }.to raise_error(
      Shoryuken::Errors::InvalidConfigurationError, /No SQS queue route configured for "orders-typo"/
    )
    expect(default.api_requests.map { |r| r[:operation_name] }).to eq([:get_queue_attributes])
  end

  it 'blocks enqueues before routes load when strict routing is enabled early, even for cached queues' do
    default.stub_responses(:get_queue_url, queue_url: east_url)
    Shoryuken::Client.queues('orders-east')
    Shoryuken::Client.enable_strict_queue_routes!
    requests_before = default.api_requests.size

    expect { TestWorker.perform_async('job', queue: 'orders-east') }.to raise_error(
      Shoryuken::Errors::InvalidConfigurationError, /No SQS queue route configured/
    )
    expect(default.api_requests.size).to eq(requests_before)

    Shoryuken.options[:sqs_queue_routes] = routes
    Shoryuken::Client.configure_queue_routes
    TestWorker.perform_async('job', queue: 'orders-east')
    expect(east.api_requests.map { |r| r[:operation_name] }).to include(:send_message)
  end

  it 'does not use a cached fallback after strict routing is enabled in options' do
    default.stub_responses(:get_queue_url, queue_url: east_url)
    Shoryuken::Client.queues('unmapped')
    Shoryuken.options[:sqs_strict_queue_routes] = true

    expect { Shoryuken::Client.queues('unmapped') }.to raise_error(
      Shoryuken::Errors::InvalidConfigurationError, /unmapped/
    )
  end

  it 'loads strict routing for producer processes from YAML' do
    Tempfile.create('shoryuken-strict-routes') do |file|
      file.write(
        YAML.dump(
          'sqs_strict_queue_routes' => true,
          'sqs_queue_routes' => { 'local' => { 'client' => nil, 'queue' => east_url } }
        )
      )
      file.flush
      Shoryuken::EnvironmentLoader.load_queue_routes(config_file: file.path)
    end

    expect(Shoryuken::Client.queues('local').client).to eq(default)
    expect { Shoryuken::Client.queues('unknown') }.to raise_error(Shoryuken::Errors::InvalidConfigurationError)
  end

  it 'validates strict mode and requires an explicit client field' do
    expect { Shoryuken::Client.configure_queue_routes(sqs_strict_queue_routes: 'true') }.to raise_error(
      Shoryuken::Errors::InvalidConfigurationError, /sqs_strict_queue_routes/
    )
    expect { Shoryuken::Client.configure_queue_routes(sqs_queue_routes: { local: { queue: east_url } }) }.to raise_error(
      Shoryuken::Errors::InvalidConfigurationError, /sqs_queue_routes.local.client/
    )
  end

  it 'rejects an unmapped polling queue during startup in strict mode' do
    Shoryuken.options[:sqs_strict_queue_routes] = true
    Shoryuken.options[:sqs_queue_routes] = routes
    Shoryuken.options[:queues] = ['orders-typo']

    expect { Shoryuken::EnvironmentLoader.new({}).check }.to raise_error(
      Shoryuken::Errors::InvalidConfigurationError, /orders-typo/
    )
    expect([default, east, west].flat_map(&:api_requests)).to be_empty
  end

  it 'selects only the requested shared credentials profile and rejects missing profiles' do
    Tempfile.create('shoryuken-aws-credentials') do |file|
      file.write(
        "[default]\naws_access_key_id=ambient\naws_secret_access_key=fake\n" \
        "[mapped]\naws_access_key_id=explicit\naws_secret_access_key=fake\n"
      )
      file.flush
      allow(Aws).to receive(:shared_config).and_return(Aws::SharedConfig.new(credentials_path: file.path))
      allow(Aws::SQS::Client).to receive(:new).and_wrap_original do |original, **options|
        original.call(**options.merge(stub_responses: true))
      end
      config = {
        sqs_clients: { profile_client: { profile: 'mapped', region: 'us-east-1' },
                       missing_client: { profile: 'absent', region: 'us-west-2' } },
        sqs_queue_routes: { mapped: { client: 'profile_client', queue: east_url },
                            missing: { client: 'missing_client', queue: west_url } }
      }
      Shoryuken::Client.configure_queue_routes(config)

      expect(Shoryuken::Client.queues('mapped').client.config.credentials.credentials.access_key_id).to eq('explicit')
      expect { Shoryuken::Client.queues('missing') }.to raise_error(
        Shoryuken::Errors::InvalidConfigurationError, /sqs_clients.missing_client.profile/
      )
      expect(default.api_requests).to be_empty
    end
  end

  it 'rejects routes to unregistered clients before sending SQS requests' do
    Shoryuken.options[:sqs_queue_routes] = { orders: { client: :typo, queue: east_url } }
    Shoryuken.options[:queues] = ['orders']

    expect do
      Shoryuken::EnvironmentLoader.new({}).check
    end.to raise_error(Shoryuken::Errors::InvalidConfigurationError, /sqs_queue_routes.orders.client/)
    expect([default, east, west].flat_map(&:api_requests)).to be_empty
  end
end
