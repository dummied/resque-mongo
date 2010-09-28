
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
    add_indexes
  end

  # Returns the current Mongo connection. If none has been created, will
  # create a new one.
  def mongo
    return @mongo if @mongo
    self.mongo = 'localhost:27017/resque'
    self.mongo
  end

  def mongo_db=(db)
      @mongo.conn.db = db
  end


  def add_indexes
    mongo_workers.create_index :worker
    mongo_stats.create_index :stat
  end

  def mongo_workers
    mongo['resque.workers']
  end

  def mongo_stats
    mongo['resque.stats']
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
    "Resque Client connected to #{mongo.connection.host}:#{mongo.connection.port}/#{mongo.name}"
  end


  #
  # queue manipulation
  #

  # Pushes a job onto a queue. Queue name should be a string and the
  # item should be any JSON-able Ruby object.
  def push(queue, item)
    mongo[queue] << item
  end

  # Pops a job off a queue. Queue name should be a string.
  #
  # Returns a Ruby object.
  def pop(queue)
    mongo[queue].find_and_modify(:sort => [:natural, :desc], :remove => true )
  rescue Mongo::OperationFailure => e
    return nil if e.message =~ /No matching object/
    raise e
  end

  # Returns an integer representing the size of a queue.
  # Queue name should be a string.
  def size(queue)
    mongo[queue].count
  end

  # Returns an array of items currently queued. Queue name should be
  # a string.
  #
  # start and count should be integer and can be used for pagination.
  # start is the item to begin, count is how many items to return.
  #
  # To get the 3rd page of a 30 item, paginatied list one would use:
  #   Resque.peek('my_list', 59, 30)
  def peek(queue, start = 0, count = 1)
    list_range(queue, start, count)
  end

  # Does the dirty work of fetching a range of items from a Redis list
  # and converting them into Ruby objects.
  def list_range(key, start = 0, count = 1)
    items = mongo[key].find({ }, { :limit => count, :skip => start}).to_a.map{ |i| i}
    count > 1 ? items : items.first
  end

  # Returns an array of all known Resque queues as strings.
  def queues    
    names = mongo.collection_names
    names.delete_if{ |name| name == 'system.indexes' || name =~ /resque\./ }  
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
    Job.create(queue_from_class(klass), klass, *args)
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
    #names.delete_if{ |name| name == 'system.indexes' || name =~ /resque\./ }   
  end

  def drop
    mongo.collections.each{ |collection| collection.drop unless collection.name =~ /^system./ }
    @mongo = nil
  end
end
