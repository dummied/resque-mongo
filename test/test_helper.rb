# -*- coding: utf-8 -*-
dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'
$TESTING = true
require 'test/unit'
require 'rubygems'
require 'resque'

begin
  require 'leftright'
rescue LoadError
end

#
# start our own redis when the tests start,
# kill it when they end
#

##
# test/spec/mini 3
# http://gist.github.com/25455
# chris@ozmm.org
#
def context(*args, &block)
  return super unless (name = args.first) && block
  require 'test/unit'
  klass = Class.new(defined?(ActiveSupport::TestCase) ? ActiveSupport::TestCase : Test::Unit::TestCase) do
    def self.test(name, &block)
      define_method("test_#{name.gsub(/\W/,'_')}", &block) if block
    end
    def self.xtest(*args) end
    def self.setup(&block) define_method(:setup, &block) end
    def self.teardown(&block) define_method(:teardown, &block) end
  end
  (class << klass; self end).send(:define_method, :name) { name.gsub(/\W/,'_') }
  klass.class_eval &block
end

##
# Helper to perform job classes
#
module PerformJob
  def perform_job(klass, *args)
    resque_job = Resque::Job.new(:testqueue, 'class' => klass, 'args' => args)
    resque_job.perform
  end
end

#
# fixture classes
#

class SomeJob
  def self.perform(repo_id, path)
  end
end

class SomeIvarJob < SomeJob
  @queue = :ivar
end

class SomeMethodJob < SomeJob
  def self.queue
    :method
  end
end

class BadJob
  def self.perform
    raise "Bad job!"
  end
end

class GoodJob
  def self.perform(name)
    "Good job, #{name}"
  end
end

class BadJobWithSyntaxError
  def self.perform
    raise SyntaxError, "Extra Bad job!"
  end
end

class UniqueJob
  @queue = :unique
  @unique_jobs = true
end

class NonUnique
  @queue = :unique

  def self.perform(data)
    "I has a #{data}"
  end

end

class OtherUnique
  @queue = :unique2
  @unique_jobs = true
  @delayed_jobs = true
end

class DelayedJob
  @queue = :delayed
  @delayed_jobs = true
  @unique_jobs = true
  def self.perform(data)
    "delayed job executing #{data.inspect}"
  end
end

class NonDelayedJob
  @queue = :delayed
end

#some redgreen fun
# -*- coding: utf-8 -*-
begin
  require 'redgreen'
  module Test
    module Unit
      module UI
        module Console
          class TestRunner
            def test_started(name)
              @individual_test_start_time = Time.now
              output_single(name + ": ", VERBOSE)
            end
            
            def test_finished(name)
              elapsed_test_time = Time.now - @individual_test_start_time
              char_to_output = elapsed_test_time > 1 ? "☻" : "."
              output_single(char_to_output, PROGRESS_ONLY) unless (@already_outputted)
              nl(VERBOSE)
              @already_outputted = false
            end
          end
        end
      end
    end
  end
  
  # -*- coding: utf-8 -*-
  class Test::Unit::UI::Console::RedGreenTestRunner < Test::Unit::UI::Console::TestRunner  
    def output_single(something, level=NORMAL)
      return unless (output?(level))
      something = case something
                  when '.' then Color.green('.')
                  when '☻' then Color.green('☻')
                  when 'F' then Color.red("F")
                  when 'E' then Color.yellow("E")
                  when '+' then Color.green('+')
                  else something
                  end
      @io.write(something) 
      @io.flush
    end
  end
rescue LoadError
  puts "consider gem install redgreen"
end


