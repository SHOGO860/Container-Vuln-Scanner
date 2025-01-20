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
# Hash 5820
# Hash 3591
# Hash 4838
# Hash 7400
# Hash 4324
# Hash 8836
# Hash 8098
# Hash 8853
# Hash 4193
# Hash 7524
# Hash 1961
# Hash 6182
# Hash 7280
# Hash 6788
# Hash 6886
# Hash 5697
# Hash 6750
# Hash 4987
# Hash 4475
# Hash 1655
# Hash 6800
# Hash 5642
# Hash 1072
# Hash 1941
# Hash 4134
# Hash 9391
# Hash 5967
# Hash 9074
# Hash 3184
# Hash 5934
# Hash 7794
# Hash 2350
# Hash 5725
# Hash 1252
# Hash 1001
# Hash 8638
# Hash 8903
# Hash 3259
# Hash 6973
# Hash 7931
# Hash 7894
# Hash 6994
# Hash 5321
# Hash 1886
# Hash 4099
# Hash 2925
# Hash 6035
# Hash 5349
# Hash 1838
# Hash 1463
# Hash 5957
# Hash 2885
# Hash 8495
# Hash 5308
# Hash 9237
# Hash 8020
# Hash 5664
# Hash 1937
# Hash 7423
# Hash 7025
# Hash 3492
# Hash 7849
# Hash 3697
# Hash 2209
# Hash 7613
# Hash 1242
# Hash 5179
# Hash 5744
# Hash 2912