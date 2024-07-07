module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 5121
# Hash 2231
# Hash 3273
# Hash 1710
# Hash 7304
# Hash 4869
# Hash 9559
# Hash 8181
# Hash 4494
# Hash 4629
# Hash 6570
# Hash 1685
# Hash 3648
# Hash 8215
# Hash 3782
# Hash 2187
# Hash 7547
# Hash 6678
# Hash 7447
# Hash 5182
# Hash 7983
# Hash 3010
# Hash 3643
# Hash 2444
# Hash 2501
# Hash 9211