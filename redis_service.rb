require 'rubygems'
require 'bundler/setup'
Bundler.require

class RedisService
  HOST = "127.0.0.1"
  PORT = 6379
  SIMPLE_DATA_TYPES = [Hash, String, Fixnum, Float]
  COMPLEX_DATA_TYPES = [Array, Hash]

  def initialize
    @db = Redis.new(host: HOST, port: PORT)
  end

  def write_data(data, dataset_name)
    # potential datatypes: string, fixnum, float, array, hash
    data.each do |k, v|
      redis_key = "#{dataset_name}:#{k}"

      if SIMPLE_DATA_TYPES.include?(v.class)
        @db.set(redis_key, v)
      # elsif v.class == Array
      #   v.each_with_index do |el, i|
      #     @db.lset(redis_key, i, el)
      #   end
      # elsif v.class == Hash
      #   @db.hmset(redis_key, v)
      else
        log_unsupported_data(v)
        stringified_data = v.to_json
        @db.set(redis_key, stringified_data)
      end
    end
  end

  def retrieve_data(dataset_names)
    data = {}
    dataset_names.each do |name|
      data[name] = @db.get(name)
    end
    data
  end

  def log_unsupported_data(value)
    File.open('ruby-redis.log','w+') do |f|
      msg = "#{Time.now} : UNSUPPORTED DATA TYPE: #{value.class}\n"
      msg += "\t#{v}\n"
      f.write(msg)
    end
  end
end
