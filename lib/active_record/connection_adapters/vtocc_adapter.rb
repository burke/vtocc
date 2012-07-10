require 'active_record/connection_adapters/abstract_adapter'
require 'active_support/core_ext/kernel/requires'
require 'active_support/core_ext/object/blank'
require 'set'
require 'vtocc'

module ActiveRecord
  class Base
    def self.vtocc_connection(config)
      host = config[:host]
      port = config[:port]
      database = config[:database]

      timeout = config[:timeout] || 5

      unless host && port && database
        raise "Vtocc requires host, port, and database."
      end

      connection_options = ["#{host}:#{port}", 5, dbname: database]

      connection = Vtocc::VtoccConnection.new(*connection_options)

      ConnectionAdapters::VtoccAdapter.new(connection, logger, connection_options, config)
    end
  end
  module ConnectionAdapters
    class VtoccAdapter < AbstractAdapter

      ADAPTER_NAME = "Vtocc".freeze

      QUOTED_TRUE, QUOTED_FALSE = '1'.freeze, '0'.freeze

      # hoping this works like mysql with magic and such.
      NATIVE_DATABASE_TYPES = {
        :primary_key => "int(11) DEFAULT NULL auto_increment PRIMARY KEY".freeze,
        :string      => { :name => "varchar", :limit => 255 },
        :text        => { :name => "text" },
        :integer     => { :name => "int", :limit => 4 },
        :float       => { :name => "float" },
        :decimal     => { :name => "decimal" },
        :datetime    => { :name => "datetime" },
        :timestamp   => { :name => "datetime" },
        :time        => { :name => "time" },
        :date        => { :name => "date" },
        :binary      => { :name => "blob" },
        :boolean     => { :name => "tinyint", :limit => 1 }
      }

      def initialize(connection, logger, connection_options, config)
        super(connection, logger)
        @cursor = connection.cursor
        @config = config
        @connection_options = connection_options
        @quoted_column_names, @quoted_table_names = {}, {}
      end

      ##
      # :singleton-method:
      # By default, the MysqlAdapter will consider all columns of type <tt>tinyint(1)</tt>
      # as boolean. If you wish to disable this emulation (which was the default
      # behavior in versions 0.13.1 and earlier) you can add the following line
      # to your application.rb file:
      #
      #   ActiveRecord::ConnectionAdapters::MysqlAdapter.emulate_booleans = false
      cattr_accessor :emulate_booleans
      self.emulate_booleans = true

      def adapter_name
        ADAPTER_NAME
      end

      def supports_migrations? #:nodoc:
        true
      end

      def supports_primary_key? #:nodoc:
        true
      end

      def supports_savepoints? #:nodoc:
        true
      end

      def native_database_types #:nodoc:
        NATIVE_DATABASE_TYPES
      end

      # QUOTING ==================================================

      def quote(value, column = nil)
        if value.kind_of?(String) && column && column.type == :binary && column.class.respond_to?(:string_to_binary)
          s = column.class.string_to_binary(value).unpack("H*")[0]
          "x'#{s}'"
        elsif value.kind_of?(BigDecimal)
          value.to_s("F")
        else
          super
        end
      end

      def quote_column_name(name) #:nodoc:
        @quoted_column_names[name] ||= "`#{name.to_s.gsub('`', '``')}`"
      end

      def quote_table_name(name) #:nodoc:
        @quoted_table_names[name] ||= quote_column_name(name).gsub('.', '`.`')
      end

      def quote_string(string) #:nodoc:
        @connection.quote(string)
      end

      def quoted_true
        QUOTED_TRUE
      end

      def quoted_false
        QUOTED_FALSE
      end

      # CONNECTION MANAGEMENT ====================================

      def active?
        @connection.alive?
      end

      def reconnect!
        disconnect!
        connect
      end

      def disconnect!
        @connection.close rescue nil
      end

      def reset!
        if @connection.respond_to?(:change_user)
          # See http://bugs.mysql.com/bug.php?id=33540 -- the workaround way to
          # reset the connection is to change the user to the same user.
          @connection.change_user(@config[:username], @config[:password], @config[:database])
          configure_connection
        end
      end

      private
      def connect
        # TODO Timeouts 
        # @connection.options(Mysql::OPT_CONNECT_TIMEOUT, @config[:connect_timeout]) if @config[:connect_timeout]
        # @connection.options(Mysql::OPT_READ_TIMEOUT, @config[:read_timeout]) if @config[:read_timeout]
        # @connection.options(Mysql::OPT_WRITE_TIMEOUT, @config[:write_timeout]) if @config[:write_timeout]

        # TODO: this is kind of janky. Look at how mysql adapter does it.
        @connection = Vtocc::VtoccConnection.new(*@connection_options)
        @cursor = @connection.cursor

        configure_connection
      end


      # DATABASE STATEMENTS ======================================

      def select_rows(sql, name = nil)
        @connection.query_with_result = true
        result = execute(sql, name)
        rows = []
        result.each { |row| rows << row }
        result.free
        @connection.more_results && @connection.next_result    # invoking stored procedures with CLIENT_MULTI_RESULTS requires this to tidy up else connection will be dropped
        rows
      end

      # Executes an SQL query and returns a MySQL::Result object. Note that you have to free
      # the Result object after you're done using it.
      def execute(sql, name = nil) #:nodoc:
        if name == :skip_logging
          @cursor.execute(sql)
        else
          log(sql, name) { @cursor.execute(sql) }
        end
      end

      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil) #:nodoc:
        super sql, name
        id_value || @connection.insert_id
      end
      alias :create :insert_sql

      def update_sql(sql, name = nil) #:nodoc:
        super
        @connection.affected_rows
      end

      def begin_db_transaction #:nodoc:
        @connection.begin
      end

      def commit_db_transaction #:nodoc:
        @connection.commit
      end

      def rollback_db_transaction #:nodoc:
        @connection.rollback
      end

      def create_savepoint
        execute("SAVEPOINT #{current_savepoint_name}")
      end

      def rollback_to_savepoint
        execute("ROLLBACK TO SAVEPOINT #{current_savepoint_name}")
      end

      def release_savepoint
        execute("RELEASE SAVEPOINT #{current_savepoint_name}")
      end

      def add_limit_offset!(sql, options) #:nodoc:
        limit, offset = options[:limit], options[:offset]
        if limit && offset
          sql << " LIMIT #{offset.to_i}, #{sanitize_limit(limit)}"
        elsif limit
          sql << " LIMIT #{sanitize_limit(limit)}"
        elsif offset
          sql << " OFFSET #{offset.to_i}"
        end
        sql
      end



      def structure_dump
        raise NotImplementedError
      end



      def debug_vars
        url = "http://#{@config['host']}:#{@config['port']}/debug/vars"
        JSON.load(Net::HTTP.get(URI.parse url))
      end

      def version
        debug_vars['Version']
      end

    end
  end
end




module ActiveRecord
  module ConnectionAdapters
    class MysqlColumn < Column #:nodoc:
      def extract_default(default)
        if sql_type =~ /blob/i || type == :text
          if default.blank?
            return null ? nil : ''
          else
            raise ArgumentError, "#{type} columns cannot have a default value: #{default.inspect}"
          end
        elsif missing_default_forged_as_empty_string?(default)
          nil
        else
          super
        end
      end

      def has_default?
        return false if sql_type =~ /blob/i || type == :text #mysql forbids defaults on blob and text columns
        super
      end

      private
        def simplified_type(field_type)
          return :boolean if MysqlAdapter.emulate_booleans && field_type.downcase.index("tinyint(1)")
          return :string  if field_type =~ /enum/i
          super
        end

        def extract_limit(sql_type)
          case sql_type
          when /blob|text/i
            case sql_type
            when /tiny/i
              255
            when /medium/i
              16777215
            when /long/i
              2147483647 # mysql only allows 2^31-1, not 2^32-1, somewhat inconsistently with the tiny/medium/normal cases
            else
              super # we could return 65535 here, but we leave it undecorated by default
            end
          when /^bigint/i;    8
          when /^int/i;       4
          when /^mediumint/i; 3
          when /^smallint/i;  2
          when /^tinyint/i;   1
          else
            super
          end
        end

        # MySQL misreports NOT NULL column default when none is given.
        # We can't detect this for columns which may have a legitimate ''
        # default (string) but we can for others (integer, datetime, boolean,
        # and the rest).
        #
        # Test whether the column has default '', is not null, and is not
        # a type allowing default ''.
        def missing_default_forged_as_empty_string?(default)
          type != :string && !null && default == ''
        end
    end

    # The MySQL adapter will work with both Ruby/MySQL, which is a Ruby-based MySQL adapter that comes bundled with Active Record, and with
    # the faster C-based MySQL/Ruby adapter (available both as a gem and from http://www.tmtm.org/en/mysql/ruby/).
    #
    # Options:
    #
    # * <tt>:host</tt> - Defaults to "localhost".
    # * <tt>:port</tt> - Defaults to 3306.
    # * <tt>:socket</tt> - Defaults to "/tmp/mysql.sock".
    # * <tt>:username</tt> - Defaults to "root"
    # * <tt>:password</tt> - Defaults to nothing.
    # * <tt>:database</tt> - The name of the database. No default, must be provided.
    # * <tt>:encoding</tt> - (Optional) Sets the client encoding by executing "SET NAMES <encoding>" after connection.
    # * <tt>:reconnect</tt> - Defaults to false (See MySQL documentation: http://dev.mysql.com/doc/refman/5.0/en/auto-reconnect.html).
    # * <tt>:sslca</tt> - Necessary to use MySQL with an SSL connection.
    # * <tt>:sslkey</tt> - Necessary to use MySQL with an SSL connection.
    # * <tt>:sslcert</tt> - Necessary to use MySQL with an SSL connection.
    # * <tt>:sslcapath</tt> - Necessary to use MySQL with an SSL connection.
    # * <tt>:sslcipher</tt> - Necessary to use MySQL with an SSL connection.
    #
    class MysqlAdapter < AbstractAdapter

      LOST_CONNECTION_ERROR_MESSAGES = [
        "Server shutdown in progress",
        "Broken pipe",
        "Lost connection to MySQL server during query",
        "MySQL server has gone away" ]


      # SCHEMA STATEMENTS ========================================

      def recreate_database(name, options = {}) #:nodoc:
        drop_database(name)
        create_database(name, options)
      end

      # Create a new MySQL database with optional <tt>:charset</tt> and <tt>:collation</tt>.
      # Charset defaults to utf8.
      #
      # Example:
      #   create_database 'charset_test', :charset => 'latin1', :collation => 'latin1_bin'
      #   create_database 'matt_development'
      #   create_database 'matt_development', :charset => :big5
      def create_database(name, options = {})
        if options[:collation]
          execute "CREATE DATABASE `#{name}` DEFAULT CHARACTER SET `#{options[:charset] || 'utf8'}` COLLATE `#{options[:collation]}`"
        else
          execute "CREATE DATABASE `#{name}` DEFAULT CHARACTER SET `#{options[:charset] || 'utf8'}`"
        end
      end

      def drop_database(name) #:nodoc:
        execute "DROP DATABASE IF EXISTS `#{name}`"
      end

      def current_database
        select_value 'SELECT DATABASE() as db'
      end

      # Returns the database character set.
      def charset
        show_variable 'character_set_database'
      end

      # Returns the database collation strategy.
      def collation
        show_variable 'collation_database'
      end

      def tables(name = nil, database = nil) #:nodoc:
        sql = "SHOW TABLES "
        sql << "IN #{quote_table_name(database)} " if database

        result = execute(sql, 'SCHEMA')
        tables = result.collect { |field| field[0] }
        result.free
        tables
      end

      def table_exists?(name)
        return true if super

        name          = name.to_s
        schema, table = name.split('.', 2)

        unless table # A table was provided without a schema
          table  = schema
          schema = nil
        end

        tables(nil, schema).include? table
      end

      def drop_table(table_name, options = {})
        super(table_name, options)
      end

      def indexes(table_name, name = nil)#:nodoc:
        indexes = []
        current_index = nil
        result = execute("SHOW KEYS FROM #{quote_table_name(table_name)}", name)
        result.each do |row|
          if current_index != row[2]
            next if row[2] == "PRIMARY" # skip the primary key
            current_index = row[2]
            indexes << IndexDefinition.new(row[0], row[2], row[1] == "0", [], [])
          end

          indexes.last.columns << row[4]
          indexes.last.lengths << row[7]
        end
        result.free
        indexes
      end

      def columns(table_name, name = nil)#:nodoc:
        sql = "SHOW FIELDS FROM #{quote_table_name(table_name)}"
        columns = []
        result = execute(sql, :skip_logging)
        result.each { |field| columns << MysqlColumn.new(field[0], field[4], field[1], field[2] == "YES") }
        result.free
        columns
      end

      def create_table(table_name, options = {}) #:nodoc:
        super(table_name, options.reverse_merge(:options => "ENGINE=InnoDB"))
      end

      def rename_table(table_name, new_name)
        execute "RENAME TABLE #{quote_table_name(table_name)} TO #{quote_table_name(new_name)}"
      end

      def add_column(table_name, column_name, type, options = {})
        add_column_sql = "ALTER TABLE #{quote_table_name(table_name)} ADD #{quote_column_name(column_name)} #{type_to_sql(type, options[:limit], options[:precision], options[:scale])}"
        add_column_options!(add_column_sql, options)
        add_column_position!(add_column_sql, options)
        execute(add_column_sql)
      end

      def change_column_default(table_name, column_name, default) #:nodoc:
        column = column_for(table_name, column_name)
        change_column table_name, column_name, column.sql_type, :default => default
      end

      def change_column_null(table_name, column_name, null, default = nil)
        column = column_for(table_name, column_name)

        unless null || default.nil?
          execute("UPDATE #{quote_table_name(table_name)} SET #{quote_column_name(column_name)}=#{quote(default)} WHERE #{quote_column_name(column_name)} IS NULL")
        end

        change_column table_name, column_name, column.sql_type, :null => null
      end

      def change_column(table_name, column_name, type, options = {}) #:nodoc:
        column = column_for(table_name, column_name)

        unless options_include_default?(options)
          options[:default] = column.default
        end

        unless options.has_key?(:null)
          options[:null] = column.null
        end

        change_column_sql = "ALTER TABLE #{quote_table_name(table_name)} CHANGE #{quote_column_name(column_name)} #{quote_column_name(column_name)} #{type_to_sql(type, options[:limit], options[:precision], options[:scale])}"
        add_column_options!(change_column_sql, options)
        add_column_position!(change_column_sql, options)
        execute(change_column_sql)
      end

      def rename_column(table_name, column_name, new_column_name) #:nodoc:
        options = {}
        if column = columns(table_name).find { |c| c.name == column_name.to_s }
          options[:default] = column.default
          options[:null] = column.null
        else
          raise ActiveRecordError, "No such column: #{table_name}.#{column_name}"
        end
        current_type = select_one("SHOW COLUMNS FROM #{quote_table_name(table_name)} LIKE '#{column_name}'")["Type"]
        rename_column_sql = "ALTER TABLE #{quote_table_name(table_name)} CHANGE #{quote_column_name(column_name)} #{quote_column_name(new_column_name)} #{current_type}"
        add_column_options!(rename_column_sql, options)
        execute(rename_column_sql)
      end

      # Maps logical Rails types to MySQL-specific data types.
      def type_to_sql(type, limit = nil, precision = nil, scale = nil)
        return super unless type.to_s == 'integer'

        case limit
        when 1; 'tinyint'
        when 2; 'smallint'
        when 3; 'mediumint'
        when nil, 4, 11; 'int(11)'  # compatibility with MySQL default
        when 5..8; 'bigint'
        else raise(ActiveRecordError, "No integer type has byte size #{limit}")
        end
      end

      def add_column_position!(sql, options)
        if options[:first]
          sql << " FIRST"
        elsif options[:after]
          sql << " AFTER #{quote_column_name(options[:after])}"
        end
      end

      # SHOW VARIABLES LIKE 'name'
      def show_variable(name)
        variables = select_all("SHOW VARIABLES LIKE '#{name}'")
        variables.first['Value'] unless variables.empty?
      end

      # Returns a table's primary key and belonging sequence.
      def pk_and_sequence_for(table) #:nodoc:
        keys = []
        result = execute("describe #{quote_table_name(table)}")
        result.each_hash do |h|
          keys << h["Field"]if h["Key"] == "PRI"
        end
        result.free
        keys.length == 1 ? [keys.first, nil] : nil
      end

      # Returns just a table's primary key
      def primary_key(table)
        pk_and_sequence = pk_and_sequence_for(table)
        pk_and_sequence && pk_and_sequence.first
      end

      def case_sensitive_equality_operator
        "= BINARY"
      end

      def limited_update_conditions(where_sql, quoted_table_name, quoted_primary_key)
        where_sql
      end

      protected
        def quoted_columns_for_index(column_names, options = {})
          length = options[:length] if options.is_a?(Hash)

          quoted_column_names = case length
          when Hash
            column_names.map {|name| length[name] ? "#{quote_column_name(name)}(#{length[name]})" : quote_column_name(name) }
          when Fixnum
            column_names.map {|name| "#{quote_column_name(name)}(#{length})"}
          else
            column_names.map {|name| quote_column_name(name) }
          end
        end

        def translate_exception(exception, message)
          return super unless exception.respond_to?(:errno)

          case exception.errno
          when 1062
            RecordNotUnique.new(message, exception)
          when 1452
            InvalidForeignKey.new(message, exception)
          else
            super
          end
        end

      private

        def configure_connection
          encoding = @config[:encoding]
          execute("SET NAMES '#{encoding}'", :skip_logging) if encoding

          # By default, MySQL 'where id is null' selects the last inserted id.
          # Turn this off. http://dev.rubyonrails.org/ticket/6778
          execute("SET SQL_AUTO_IS_NULL=0", :skip_logging)
        end

        def select(sql, name = nil)
          @connection.query_with_result = true
          result = execute(sql, name)
          rows = []
          result.each_hash { |row| rows << row }
          result.free
          @connection.more_results && @connection.next_result    # invoking stored procedures with CLIENT_MULTI_RESULTS requires this to tidy up else connection will be dropped
          rows
        end

        def supports_views?
          version[0] >= 5
        end

        def version
          @version ||= @connection.server_info.scan(/^(\d+)\.(\d+)\.(\d+)/).flatten.map { |v| v.to_i }
        end

        def column_for(table_name, column_name)
          unless column = columns(table_name).find { |c| c.name == column_name.to_s }
            raise "No such column: #{table_name}.#{column_name}"
          end
          column
        end
    end
  end
end

