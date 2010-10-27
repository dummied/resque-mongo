require File.dirname(__FILE__) + '/test_helper'

context "Resque" do
  setup do
    Resque.drop
    Resque.bypass_queues = false
    Resque.enable_delay(:delayed)
    Resque.push(:people, { 'name' => 'chris' })
    Resque.push(:people, { 'name' => 'bob' })
    Resque.push(:people, { 'name' => 'mark' })
  end
  
  test "can set a namespace through a url-like string" do
    assert Resque.mongo
    assert_equal 'resque', Resque.mongo.name
    Resque.mongo = 'localhost:27017/namespace'
    assert_equal 'namespace', Resque.mongo.name
  end

  test "can put jobs on a queue" do
    assert Resque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Resque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
  end

  test "can grab jobs off a queue" do
    Resque::Job.create(:jobs, 'some-job', 20, '/tmp')

    job = Resque.reserve(:jobs)

    assert_kind_of Resque::Job, job
    assert_equal SomeJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]
  end

  test "can re-queue jobs" do
    Resque::Job.create(:jobs, 'some-job', 20, '/tmp')

    job = Resque.reserve(:jobs)
    job.recreate

    assert_equal job, Resque.reserve(:jobs)
  end

  test "can put jobs on a queue by way of an ivar" do
    assert_equal 0, Resque.size(:ivar)
    assert Resque.enqueue(SomeIvarJob, 20, '/tmp')
    assert Resque.enqueue(SomeIvarJob, 20, '/tmp')

    job = Resque.reserve(:ivar)

    assert_kind_of Resque::Job, job
    assert_equal SomeIvarJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]

    assert Resque.reserve(:ivar)
    assert_equal nil, Resque.reserve(:ivar)
  end

  test "can remove jobs from a queue by way of an ivar" do
    assert_equal 0, Resque.size(:ivar)
    assert Resque.enqueue(SomeIvarJob, 20, '/tmp')
    assert Resque.enqueue(SomeIvarJob, 30, '/tmp')
    assert Resque.enqueue(SomeIvarJob, 20, '/tmp')
    assert Resque::Job.create(:ivar, 'blah-job', 20, '/tmp')
    assert Resque.enqueue(SomeIvarJob, 20, '/tmp')
    assert_equal 5, Resque.size(:ivar)

    assert Resque.dequeue(SomeIvarJob, 30, '/tmp')
    assert_equal 4, Resque.size(:ivar)
    assert Resque.dequeue(SomeIvarJob)
    assert_equal 1, Resque.size(:ivar)
  end

  test "jobs have a nice #inspect" do
    assert Resque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    job = Resque.reserve(:jobs)
    assert_equal '(Job{jobs} | SomeJob | [20, "/tmp"])', job.inspect
  end

  test "jobs can be destroyed" do
    assert Resque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Resque::Job.create(:jobs, 'BadJob', 20, '/tmp')
    assert Resque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Resque::Job.create(:jobs, 'BadJob', 30, '/tmp')
    assert Resque::Job.create(:jobs, 'BadJob', 20, '/tmp')

    assert_equal 5, Resque.size(:jobs)
    assert_equal 2, Resque::Job.destroy(:jobs, 'SomeJob')
    assert_equal 3, Resque.size(:jobs)
    assert_equal 1, Resque::Job.destroy(:jobs, 'BadJob', 30, '/tmp')
    assert_equal 2, Resque.size(:jobs)
  end

  test "jobs can test for equality" do
    assert Resque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Resque::Job.create(:jobs, 'some-job', 20, '/tmp')
    assert_equal Resque.reserve(:jobs), Resque.reserve(:jobs)

    assert Resque::Job.create(:jobs, 'SomeMethodJob', 20, '/tmp')
    assert Resque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert_not_equal Resque.reserve(:jobs), Resque.reserve(:jobs)

    assert Resque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Resque::Job.create(:jobs, 'SomeJob', 30, '/tmp')
    assert_not_equal Resque.reserve(:jobs), Resque.reserve(:jobs)
  end

  test "can put jobs on a queue by way of a method" do
    assert_equal 0, Resque.size(:method)
    assert Resque.enqueue(SomeMethodJob, 20, '/tmp')
    assert Resque.enqueue(SomeMethodJob, 20, '/tmp')

    job = Resque.reserve(:method)

    assert_kind_of Resque::Job, job
    assert_equal SomeMethodJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]

    assert Resque.reserve(:method)
    assert_equal nil, Resque.reserve(:method)
  end

  test "needs to infer a queue with enqueue" do
    assert_raises Resque::NoQueueError do
      Resque.enqueue(SomeJob, 20, '/tmp')
    end
  end

  test "can put items on a queue" do
    assert Resque.push(:people, { 'name' => 'jon' })
  end

  def pop_no_id(queue)
    item = Resque.pop(queue)
    item.delete("_id")
    item
  end


  test "can pull items off a queue" do
    assert_equal({ 'name' => 'chris' }, pop_no_id(:people))
    assert_equal({ 'name' => 'bob' }, pop_no_id(:people)) 
    assert_equal({ 'name' => 'mark' }, pop_no_id(:people))
    assert_equal nil, Resque.pop(:people)
  end

  test "knows how big a queue is" do
    assert_equal 3, Resque.size(:people)

    assert_equal({ 'name' => 'chris' }, pop_no_id(:people))
    assert_equal 2, Resque.size(:people)

    assert_equal({ 'name' => 'bob' }, pop_no_id(:people))
    assert_equal({ 'name' => 'mark' }, pop_no_id(:people))
    assert_equal 0, Resque.size(:people)
  end

  test "can peek at a queue" do
    peek = Resque.peek(:people)
    peek.delete "_id"
    assert_equal({ 'name' => 'chris' }, peek)
    assert_equal 3, Resque.size(:people)
  end

  test "can peek multiple items on a queue" do
    assert_equal('bob', Resque.peek(:people, 1, 1)['name'])
    peek = Resque.peek(:people, 1, 2).map {  |hash| { 'name' => hash['name']}}
    assert_equal([{ 'name' => 'bob' }, { 'name' => 'mark' }], peek)
    peek = Resque.peek(:people, 0, 2).map {  |hash| { 'name' => hash['name']} }
    assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }], peek)
    peek = Resque.peek(:people, 0, 3).map {  |hash| { 'name' => hash['name']} }
    assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }, { 'name' => 'mark' }], peek)
    peek = Resque.peek(:people, 2, 1)
    assert_equal('mark', peek['name'])
    assert_equal nil, Resque.peek(:people, 3)
    assert_equal [], Resque.peek(:people, 3, 2)
  end

  test "knows what queues it is managing" do
    assert_equal %w( people ), Resque.queues
    Resque.push(:cars, { 'make' => 'bmw' })
    assert_equal %w( cars people ), Resque.queues.sort
  end

  test "queues are always a list" do
    Resque.drop
    assert_equal [], Resque.queues
  end

  test "can delete a queue" do
    Resque.push(:cars, { 'make' => 'bmw' })
    assert_equal %w( cars people ), Resque.queues.sort
    Resque.remove_queue(:people)
    assert_equal %w( cars ), Resque.queues
    assert_equal nil, Resque.pop(:people)
  end

  test "keeps track of resque keys" do
    assert Resque.keys.include? 'people'
  end

  test "badly wants a class name, too" do
    assert_raises Resque::NoClassError do
      Resque::Job.create(:jobs, nil)
    end
  end

  test "keeps stats" do
    Resque::Job.create(:jobs, SomeJob, 20, '/tmp')
    Resque::Job.create(:jobs, BadJob)
    Resque::Job.create(:jobs, GoodJob)

    Resque::Job.create(:others, GoodJob)
    Resque::Job.create(:others, GoodJob)

    stats = Resque.info
    assert_equal 8, stats[:pending]

    @worker = Resque::Worker.new(:jobs)
    @worker.register_worker
    2.times { @worker.process }

    job = @worker.reserve
    @worker.working_on job

    stats = Resque.info
    assert_equal 1, stats[:working]
    assert_equal 1, stats[:workers]

    @worker.done_working

    stats = Resque.info
    assert_equal 3, stats[:queues]
    assert_equal 3, stats[:processed]
    assert_equal 1, stats[:failed]
 #   assert_equal [Resque.redis.respond_to?(:server) ? 'localhost:9736' : 'redis://localhost:9736/0'], stats[:servers]
  end

  test "decode bad json" do
    assert_nil Resque.decode("{\"error\":\"Module not found \\u002\"}")
  end

  test "unique jobs are unique" do  
    #does uniqueness work?
    Resque.enqueue(UniqueJob, {:_id => 'my_id', :arg1=> 'my args1'})
    assert_equal(1, Resque.size(:unique))
    assert_equal('my args1',  Resque.peek(:unique)['args'][0]['arg1'])
    assert_equal('my_id',  Resque.peek(:unique)['args'][0]['_id'])
    Resque.enqueue(UniqueJob, {:_id => 'my_id',  :arg1=> 'my args2'})
    assert_equal(1, Resque.size(:unique))
    assert_equal('my args2',  Resque.peek(:unique)['args'][0]['arg1'])

    #if I enqueue unique jobs with the same key in 2 queues, do I get 2 jobs?
    Resque.enqueue(UniqueJob, {:_id => 'my_id3', :arg1=> 'my arg3'})
    assert_equal(2, Resque.size(:unique))
    Resque.enqueue(OtherUnique, {:_id => 'my_id3',  :arg1=> 'my args4'})
    #following line fails because :unique and :unique2 are in the same collection
    #\assert_equal(2, Resque.size(:unique))
    assert_equal(1, Resque.size(:unique2))
    
    #can I enqueue normal jobs in the unique queue?
    Resque.enqueue(NonUnique,  {:arg1=> 'my args'})
    assert_equal(3, Resque.size(:unique))
    Resque.enqueue(NonUnique,  {:_id => 'my_id', :_id => 'my_id', :arg1=> 'my args2'})
    assert_equal(4, Resque.size(:unique))

    #how do unique jobs work without a given _id?
    Resque.enqueue(UniqueJob, {:arg1=> 'my args3'})
    assert_equal(5, Resque.size(:unique))
    assert_equal('my args3',  Resque.peek(:unique, 4)['args'][0]['arg1'])
    Resque.enqueue(UniqueJob, {:arg1=> 'my args4'})
    assert_equal(6, Resque.size(:unique))
    assert_equal('my args4',  Resque.peek(:unique, 5)['args'][0]['arg1'])
  end

  test "Can bypass queues for testing" do
    Resque.enqueue(NonUnique, 'test')
    assert_equal(1, Resque.size(:unique))
    Resque.bypass_queues = true
    Resque.enqueue(NonUnique, 'test')
    assert_equal(1, Resque.size(:unique))
    Resque.bypass_queues = false
    Resque.enqueue(NonUnique, 'test')
    assert_equal(2, Resque.size(:unique))    
  end

  test "delayed jobs work" do
    args = { :delay_until => Time.new-1}
    Resque.enqueue(DelayedJob, args)
    job = Resque::Job.reserve(:delayed)
    assert_equal(1, job.args[0].keys.length)
    assert_equal(args[:delay_until].to_i, job.args[0]["delay_until"].to_i)
    args[:delay_until] = Time.new + 2
    assert_equal(0, Resque.delayed_size(:delayed))
    Resque.enqueue(DelayedJob, args)
    
    assert_equal(1, Resque.delayed_size(:delayed))
    assert_nil Resque.peek(:delayed)
    assert_nil Resque::Job.reserve(:delayed)
    sleep 1
    assert_nil Resque::Job.reserve(:delayed)
    sleep 1
    assert_equal(DelayedJob, Resque::Job.reserve(:delayed).payload_class)
  end

  test "delayed unique jobs modify args in place" do
    args = { :delay_until => Time.new + 3600, :_id => 'unique'}
    Resque.enqueue(DelayedJob, args)
    assert_nil(Resque.peek(:delayed))
    args[:delay_until] = Time.new - 1
    Resque.enqueue(DelayedJob, args)
    assert_equal(2, Resque::Job.reserve(:delayed).args[0].keys.count)
  end

  test "delayed attribute is ignored when bypassing queues" do
    Resque.bypass_queues = true
    args = { :delay_until => Time.new+20}
    foo =  Resque.enqueue(DelayedJob, args)
    assert(0, Resque.size(:delayed))
    assert(args[:delay_until] > Time.new)
    assert(foo =~ /^delayed job executing/)
    Resque.bypass_queues = false
  end

  test "mixing delay and non-delay is bad" do
    dargs = { :delay_until => Time.new + 3600}
    
    #non-delay into delay
    assert_raise(Resque::QueueError) do
      Resque.enqueue(NonDelayedJob, dargs)
    end
    
    #delay into non-delay
    assert_raise(Resque::QueueError) do
      Resque.enqueue(MistargetedDelayedJob, dargs)
    end
  end
end
