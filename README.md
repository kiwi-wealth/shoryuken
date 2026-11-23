# Shoryuken

Shoryuken _sho-ryu-ken_ is a super-efficient [Amazon SQS](https://aws.amazon.com/sqs/) thread-based message processor.

[![Build Status](https://github.com/ruby-shoryuken/shoryuken/workflows/Specs/badge.svg)](https://github.com/ruby-shoryuken/shoryuken/actions)
[![Join the chat at https://slack.shoryuken.io](https://raw.githubusercontent.com/karafka/misc/master/slack.svg)](https://slack.shoryuken.io)

## Key features

- [Rails Active Job](https://github.com/ruby-shoryuken/shoryuken/wiki/Rails-Integration-Active-Job)
- [Queue Load balancing](https://github.com/ruby-shoryuken/shoryuken/wiki/Shoryuken-options#load-balancing)
- [Concurrency per queue](https://github.com/ruby-shoryuken/shoryuken/wiki/Processing-Groups)
- [Long Polling](https://github.com/ruby-shoryuken/shoryuken/wiki/Long-Polling)
- [Batch processing](https://github.com/ruby-shoryuken/shoryuken/wiki/Worker-options#batch)
- [Auto extend visibility timeout](https://github.com/ruby-shoryuken/shoryuken/wiki/Worker-options#auto_visibility_timeout)
- [Exponential backoff](https://github.com/ruby-shoryuken/shoryuken/wiki/Worker-options#retry_intervals)
- [Middleware support](https://github.com/ruby-shoryuken/shoryuken/wiki/Middleware)
- Amazon SQS CLI. See `shoryuken help sqs`
- Configuration check without processing. See `shoryuken help check`

## Requirements

Ruby 3.3 or greater.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'shoryuken'
```

And then execute:

```shell
$ bundle
```

## Usage

Check the [Getting Started](https://github.com/ruby-shoryuken/shoryuken/wiki/Getting-Started) page.

### Different AWS credentials per queue

Define named SQS clients and map queue aliases to them in `shoryuken.yml`. Aliases let one process use two queues with the same AWS name:

```yaml
sqs_clients:
  east: { profile: orders-east, region: us-east-1 }
  west: { profile: orders-west, region: us-west-2 }
sqs_queue_routes:
  orders-east:
    client: east
    queue: https://sqs.us-east-1.amazonaws.com/111111111111/orders
  orders-west:
    client: west
    queue: https://sqs.us-west-2.amazonaws.com/222222222222/orders
queues:
  - [orders-east, 1]
  - [orders-west, 1]
```

Use the alias in `shoryuken_options queue:`, enqueue overrides, and Active Job's `queue_as`. If Active Job adds a prefix, use the resulting name (for example `myapp_orders-east`) as the route key and configure the polling entry according to the existing prefixing settings. Route keys are exact: a physical queue name, URL, or ARN does not implicitly match an alias.

The worker loads routes from its config file before it starts polling. In a separate producer process, load the same routes before the first enqueue:

```ruby
Shoryuken::EnvironmentLoader.load_queue_routes(config_file: Rails.root.join('config/shoryuken.yml'))
```

YAML profiles read from the AWS shared credentials file, not the SDK's default credential chain. For assume-role, SSO, or another provider, register a prebuilt `Aws::SQS::Client` with `Shoryuken.sqs_named_clients[:name] = client` and reference that name in a route without defining it again under `sqs_clients`. Register it in an initializer or a file loaded by `--require`. Routes become active after those files load, so don't enqueue from their top-level code. In a Ruby-only setup, set `Shoryuken.options[:sqs_queue_routes]` and call `Shoryuken::Client.configure_queue_routes` before enqueueing or polling.

Unmapped identifiers use the existing default client. Mapped queues never fall back to it, and `shoryuken sqs` administration is unchanged. Grant each identity only the operations it needs: for example, attribute reads, receives, deletes, and visibility changes for consumers, or attribute reads and sends for producers. For targets specified by name, also allow `GetQueueUrl`. A single process has access to all configured identities; use separate processes if you need process-level isolation.

## More Information

For more information check the [wiki page](https://github.com/ruby-shoryuken/shoryuken/wiki).

## Credits

[Mike Perham](https://github.com/mperham), creator of [Sidekiq](https://github.com/mperham/sidekiq), and [everybody who contributed to it](https://github.com/mperham/sidekiq/graphs/contributors). Shoryuken wouldn't exist as it is without those contributions.

## License

Copyright (c) Pablo Cantero\
Copyright (c) Maciej Mensfeld\
Copyright (c) [Shoryuken contributors](https://github.com/ruby-shoryuken/shoryuken/graphs/contributors)

Shoryuken is an Open Source project licensed under the terms of the LGPLv3 license. See [LICENSE](LICENSE) for the license text.

## Contributing

1. Fork it ( https://github.com/ruby-shoryuken/shoryuken/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

### Testing

To run all unit specs against the latest dependency versions, execute

```sh
bundle exec rake spec
```

To run integration specs (including Rails tests), start ElasticMQ and run:

```sh
docker compose up -d
bundle exec rake spec:integration
```

### To release a new version

Compare latest tag with HEAD:

```sh
git log $(git describe --tags --abbrev=0)..HEAD --oneline
```

then update CHANGELOG.md.

Update version in `lib/shoryuken/version.rb` with the appropriate version number [SEMVER](https://semver.org/).

then run:

```sh
bundle exec rake release
```
