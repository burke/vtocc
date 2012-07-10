$:.unshift File.expand_path("../../lib", __FILE__)
require 'open3'
require 'yaml'
require 'mysql2'
require 'json'
require 'net/http'

require 'vtocc'

class Runner < MiniTest::Unit
  def before_suites;end
  def after_suites;end
  def _run_suites(suites, type)
    begin
      before_suites
      super(suites, type)
    ensure
      after_suites
    end
  end

  def _run_suite(suite, type)
    begin
      suite.before_suite if suite.respond_to?(:before_suite)
      super(suite, type)
    ensure
      suite.after_suite if suite.respond_to?(:after_suite)
    end
  end
end
MiniTest::Unit.runner = Runner.new

module Vtocc
  class GenericTestCase < MiniTest::Unit::TestCase

    def debug_vars
      JSON.load(Net::HTTP.get(URI.parse 'http://localhost:9461/debug/vars'))
    end

    QUERY_LOG_FILE = "/tmp/vtocc.query.log"
    LOG_FILE       = "/tmp/vtocc.log"

    def self.dbconfig
      python_to_ruby_dbconfig(YAML.load(File.read dbconfig_path))
    end

    def self.test_schema_path
      File.expand_path('../test_schema.sql', __FILE__)
    end

    def self.dbconfig_path
      File.expand_path('../dbtest.json', __FILE__)
    end

    def self.python_to_ruby_dbconfig(config)
      {
        host: config['host'],
        port: config['port'],
        socket: config['unix_socket'],
        username: config['uname'],
        password: config['pass'],
        database: config['dbname'],
        encoding: config['charset']
      }
    end

    def self.cfg
      @@cfg
    end

    def self.conn
      @@conn
    end

    def self.mysql_conn
      @@mysql_conn
    end

    def self.before_suite
      return if defined?($before_suited)
      $before_suited = true
      vttop = ENV['VTTOP'] or raise "VTTOP not defined"
      occpath = vttop + "/go/cmd/vtocc"

      @@cfg = self.dbconfig
      @@mysql_conn = Mysql2::Client.new(@@cfg)
      @@clean_sqls = []
      @@init_sqls  = []
      clean_mode = false

      File.readlines(test_schema_path).each do |line|
        line.strip!
        line == "# clean" and clean_mode = true
        if line == '' || line =~ /^#/
          next
        end
        if clean_mode
          @@clean_sqls << line
        else
          @@init_sqls << line
        end
      end

      begin
        @@init_sqls.each do |line|
          @@mysql_conn.query(line, {})
        end
      ensure
        @@mysql_conn.close
      end

      # TODO memcached?

      occ_args = [
        vttop + "/go/cmd/vtocc/vtocc",
        "-config", File.expand_path("../occ.json", __FILE__),
        "-dbconfig", dbconfig_path,
        "-max-open-fds", "1024",
        "-logfile", LOG_FILE,
        "-querylog", QUERY_LOG_FILE
      ]

      # @vtstderr = File.open("vtocc.stderr.log", "a+")
      @@vtocc = Open3.popen3(*occ_args)

      0.upto(29).each do |i|
        begin
          @@conn = connect
          puts @@conn.inspect
          @@querylog = Tailer.new(File.open(QUERY_LOG_FILE, "r"))
          @@log = Tailer.new(File.open(LOG_FILE, "r"))
          return
        rescue Errno::ECONNREFUSED
          raise if i == 29
          sleep 1
        end
      end

    end

    def self.connect
      Vtocc::VtoccConnection.connect("localhost:9461", 2, dbname: 'vitess')
    end

    class Tailer

      def initialize(f)
        @f = f
        reset
      end

      def reset
        @f.seek(0, IO::SEEK_END)
        @pos = @f.pos
      end

      def read
        @f.seek(0, IO::SEEK_END)
        newpos = @f.pos
        return "" if newpos < @pos
        @f.seek(@pos, IO::SEEK_SET)
        size = newpos - @pos
        @pos = newpos
        @f.read(size)
      end

    end
  end
end
