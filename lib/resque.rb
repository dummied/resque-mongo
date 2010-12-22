
begin
  require 'yajl'
rescue LoadError
  require 'json'
end

require 'mongo'

require 'resque/version'

require 'resque/errors'

require 'resque/failure'
require 'resque/failure/base'

require 'resque/helpers'
require 'resque/stat'
require 'resque/job'
require 'resque/worker'
require 'resque/plugin'

module Resque
  include Helpers
  extend self
  attr_accessor :bypass_queues
  @bypass_queues = false
  @delay_allowed = []
  
  
  def mongo=(server)
    if server.is_a? String
      opts = server.split(':')
      host = opts[0]
      if opts[1] =~ /\//
        opts = opts[1].split('/')
        port = opts[0]
        queuedb = opts[1]
      else
        port = opts[1]
      end
      conn = Mongo::Connection.new host, port
    elsif server.is_a? Hash
      conn = Mongo::Connection.new(options[:server], options[:port], options)
      queuedb = options[:queuedb] || 'resque'
    elsif server.is_a? Mongo::Connection
      conn = server
    end
    queuedb ||= 'resque'
    @mongo = conn.db queuedb    
    initialize_mongo
  end

  # Returns the current Mongo connection. If none has been created, will
  # create a new one.
  def mongo
    return @mongo if @mongo
    self.mongo = 'localhost:27017/resque'
    self.mongo
  end

  def mongo_db=(db)
      mongo.conn.db = db
  end

  def mongo?
    return @mongo
  end

  def initialize_mongo
    mongo_workers.create_index :worker
    mongo_stats.create_index :stat
    delay_allowed = mongo_stats.find_one({ :stat => 'Delayable Queues'}, { :fields => ['value']})
    @delay_allowed = delay_allowed['value'].map{ |queue| queue.to_sym} if delay_allowed    
  end

  def mongo_workers
    mongo['resque.workers']
  end

  def mongo_stats
    mongo['resque.metrics']
  end

  def mongo_failures
    mongo['resque.failures']
  end

  # The `before_first_fork` hook will be run in the **parent** process
  # only once, before forking to run the first job. Be careful- any
  # changes you make will be permanent for the lifespan of the
  # worker.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def before_first_fork(&block)
    block ? (@before_first_fork = block) : @before_first_fork
  end

  # Set a proc that will be called in the parent process before the
  # worker forks for the first time.
  def before_first_fork=(before_first_fork)
    @before_first_fork = before_first_fork
  end

  # The `before_fork` hook will be run in the **parent** process
  # before every job, so be careful- any changes you make will be
  # permanent for the lifespan of the worker.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def before_fork(&block)
    block ? (@before_fork = block) : @before_fork
  end

  # Set the before_fork proc.
  def before_fork=(before_fork)
    @before_fork = before_fork
  end

  # The `after_fork` hook will be run in the child process and is passed
  # the current job. Any changes you make, therefore, will only live as
  # long as the job currently being processed.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def after_fork(&block)
    block ? (@after_fork = block) : @after_fork
  end

  # Set the after_fork proc.
  def after_fork=(after_fork)
    @after_fork = after_fork
  end

  def to_s
    connection_info = Mongo::VERSION >= "1.1.3" ? mongo.connection.primary_pool : mongo.connection
    "Resque Client connected to #{connection_info.host}:#{connection_info.port}/#{mongo.name}"
  end

  def allows_unique_jobs(klass)
    klass.instance_variable_get(:@unique_jobs) ||
      (klass.respond_to?(:unique_jobs) and klass.unique_jobs)
  end

  def allows_delayed_jobs(klass)
    klass.instance_variable_get(:@delayed_jobs) ||
      (klass.respond_to?(:delayed_jobs) and klass.delayed_jobs)
  end

  def queue_allows_delayed(queue)
    @delay_allowed.include?(queue.to_sym) || @delay_allowed.include?(queue.to_s)
  end

  def enable_delay(queue)
    unless queue_allows_delayed queue
      @delay_allowed << queue 
      mongo_stats.update({:stat => 'Delayable Queues'}, { '$addToSet' => { 'value' => queue}}, { :upsert => true})
    end
  end

  #
  # queue manipulation
  #

  # Pushes a job onto a queue. Queue name should be a string and the
  # item should be any JSON-able Ruby object.
  def push(queue, item)
    item[:resque_enqueue_timestamp] = Time.now
    if item[:unique]
      mongo[queue].update({'_id' => item[:_id]}, item, { :upsert => true})
    else
      mongo[queue] << item
    end
  end

  # Pops a job off a queue. Queue name should be a string.
  #
  # Returns a Ruby object.
  def pop(queue)
    query = { }
    if queue_allows_delayed queue
      query['delay_until'] = { '$not' => { '$gt' => Time.new}}
    end
    #sorting will result in significant performance penalties for large queues, you have been warned.
    item = mongo[queue].find_and_modify(:query => query, :remove => true, :sort => [[:_id, :asc]] )
  rescue Mongo::OperationFailure => e
    return nil if e.message =~ /No matching object/
    raise e
  end

  # Returns an integer representing the size of a queue.
  # Queue name should be a string.
  def size(queue) 
    mongo[queue].count
  end

  def delayed_size(queue)
    if queue_allows_delayed queue
      mongo[queue].find({'delay_until' => { '$gt' => Time.new}}).count
    else
      mongo[queue].count
    end
  end

  def ready_size(queue)
    if queue_allows_delayed queue
      mongo[queue].find({'delay_until' =>  { '$not' => { '$gt' => Time.new}}}).count
    else
      mongo[queue].count
    end
  end


  # Returns an array of items currently queued. Queue name should be
  # a string.
  #
  # start and count should be integer and can be used for pagination.
  # start is the item to begin, count is how many items to return.
  #
  # To get the 3rd page of a 30 item, paginatied list one would use:
  #   Resque.peek('my_list', 59, 30)
  def peek(queue, start = 0, count = 1, mode = :ready)
    list_range(queue, start, count, mode)
  end

  # Does the dirty work of fetching a range of items from a Redis list
  # and converting them into Ruby objects.
  def list_range(key, start = 0, count = 1, mode = :ready)
    query = { }
    sort = []
    if queue_allows_delayed(key)
      if mode == :ready
        query['delay_until'] = { '$not' => { '$gt' => Time.new}}
      elsif mode == :delayed
        query['delay_until'] = { '$gt' => Time.new}
      elsif mode == :delayed_sorted
        query['delay_until'] = { '$gt' => Time.new}
        sort << ['delay_until', 1]
      elsif mode == :all_sorted
        query = {}
        sort << ['delay_until', 1]
      end
    end
    items = mongo[key].find(query, { :limit => count, :skip => start, :sort => sort}).to_a.map{ |i| i}
    count > 1 ? items : items.first
  end

  # Returns an array of all known Resque queues as strings.
  def queues    
    names = mongo.collection_names
    names.delete_if{ |name| name =~ /system./ || name =~ /resque\./ }  
  end

  # Given a queue name, completely deletes the queue.
  def remove_queue(queue)
    mongo[queue].drop
  end

  #
  # job shortcuts
  #

  # This method can be used to conveniently add a job to a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # This method is considered part of the `stable` API.
  def enqueue(klass, *args)
    if @bypass_queues
      klass.send(:perform, *args)
    else
      Job.create(queue_from_class(klass), klass, *args)
    end
  end

  # This method can be used to conveniently remove a job from a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If no args are given, this method will dequeue *all* jobs matching
  # the provided class. See `Resque::Job.destroy` for more
  # information.
  #
  # Returns the number of jobs destroyed.
  #
  # Example:
  #
  #   # Removes all jobs of class `UpdateNetworkGraph`
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph)
  #
  #   # Removes all jobs of class `UpdateNetworkGraph` with matching args.
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph, 'repo:135325')
  #
  # This method is considered part of the `stable` API.
  def dequeue(klass, *args)
    Job.destroy(queue_from_class(klass), klass, *args)
  end

  # Given a class, try to extrapolate an appropriate queue based on a
  # class instance variable or `queue` method.
  def queue_from_class(klass)
    klass.instance_variable_get(:@queue) ||
      (klass.respond_to?(:queue) and klass.queue)
  end

  # This method will return a `Resque::Job` object or a non-true value
  # depending on whether a job can be obtained. You should pass it the
  # precise name of a queue: case matters.
  #
  # This method is considered part of the `stable` API.
  def reserve(queue)
    Job.reserve(queue)
  end


  #
  # worker shortcuts
  #

  # A shortcut to Worker.all
  def workers
    Worker.all
  end

  # A shortcut to Worker.working
  def working
    Worker.working
  end

  # A shortcut to unregister_worker
  # useful for command line tool
  def remove_worker(worker_id)
    worker = Resque::Worker.find(worker_id)
    worker.unregister_worker
  end

  #
  # stats
  #

  # Returns a hash, similar to redis-rb's #info, of interesting stats.
  def info
    return {
      :pending   => queues.inject(0) { |m,k| m + size(k) },
      :processed => Stat[:processed],
      :queues    => queues.size,
      :workers   => workers.size.to_i,
      :working   => working.count,
      :failed    => Stat[:failed],
      :servers   => to_s,
      :environment  => ENV['RAILS_ENV'] || ENV['RACK_ENV'] || 'development'
    }
  end

  # Returns an array of all known Resque keys in Redis. Redis' KEYS operation
  # is O(N) for the keyspace, so be careful - this can be slow for big databases.
  def keys
    names = mongo.collection_names
  end

  def drop
    mongo.collections.each{ |collection| collection.drop unless collection.name =~ /^system./ }
    @mongo = nil
  end
end
