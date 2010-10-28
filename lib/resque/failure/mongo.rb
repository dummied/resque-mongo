module Resque
  module Failure
    # A Failure backend that stores exceptions in Mongo. Very simple but
    # works out of the box, along with support in the Resque web app.
    class Mongo < Base
      def save
        data = {
          :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S"),
          :payload   => payload,
          :exception => exception.class.to_s,
          :error     => exception.to_s,
          :backtrace => Array(exception.backtrace),
          :worker    => worker.to_s,
          :queue     => queue
        }
        Resque.mongo_failures << data
      end

      def self.count
        Resque.mongo_failures.count
      end

      def self.all(start = 0, count = 1)
        
        all_failures = Resque.mongo_failures.find().skip(start.to_i).limit(count.to_i).to_a
        all_failures.size == 1 ? all_failures.first : all_failures        
      end

      def self.clear
        Resque.mongo_failures.remove
      end

      def self.requeue(index)
        item = all(index)
        item['retried_at'] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
        Resque.mongo_failures.update({ :_id => item['_id']}, item)
        Job.create(item['queue'], item['payload']['class'], *item['payload']['args'])
      end
    end
  end
end
