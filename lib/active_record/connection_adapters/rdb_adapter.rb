require 'fb'
require 'base64'
require 'arel'
require 'arel/visitors/rdb_visitor'


require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/rdb/database_statements'
require 'active_record/connection_adapters/rdb/database_limits'
require 'active_record/connection_adapters/rdb/database_tasks'
require 'active_record/connection_adapters/rdb/schema_creation'
require 'active_record/connection_adapters/rdb/schema_dumper'
require 'active_record/connection_adapters/rdb/schema_statements'
require 'active_record/connection_adapters/rdb/table_definition'
require 'active_record/connection_adapters/rdb/quoting'
require 'active_record/connection_adapters/rdb_column'
require 'active_record/rdb_base'

require 'active_record/connection_adapters/rdb/core_ext/relation'

module ActiveRecord
  module ConnectionAdapters
    class RdbAdapter < AbstractAdapter
      include Rdb::DatabaseLimits
      include Rdb::DatabaseStatements
      include Rdb::Quoting
      include Rdb::SchemaStatements
      include Rdb::ColumnDumper

      @@default_transaction_isolation = :read_committed
      cattr_accessor :default_transaction_isolation

      ADAPTER_NAME = 'rdb'.freeze

      def initialize(connection, logger = nil, config = {})
        super(connection, logger, config)
        # Our Responsibility
        @config = config
        @visitor = Arel::Visitors::Rdb.new self
      end

      def arel_visitor
        Arel::Visitors::Rdb.new self
      end

      def valid_type?(type)
        !native_database_types[type].nil?
      end

      def adapter_name
        ADAPTER_NAME
      end

      def schema_creation
        Rdb::SchemaCreation.new self
      end

      def supports_migrations?
        true
      end

      def supports_primary_key?
        true
      end

      def supports_count_distinct?
        true
      end

      def supports_ddl_transactions?
        true
      end

      def supports_transaction_isolation?
        true
      end

      def supports_savepoints?
        true
      end

      def prefetch_primary_key?(table_name = nil)
        true
      end

      def ids_in_list_limit
        1499
      end

      def combine_bind_parameters(
          from_clause: [],
          join_clause: [],
          where_clause: [],
          having_clause: [],
          limit: nil,
          offset: nil
      ) # :nodoc:
        result = from_clause + join_clause + where_clause + having_clause
        if limit && !offset
          result << limit
        end
        if offset && !limit
          result = [offset] + result
        end
        if offset && limit
          limit = limit.class.new(limit.name, limit.value + offset.value, limit.type)
          offset = offset.class.new(offset.name, offset.value + 1, offset.type)
          result << offset
          result << limit
        end
        result
      end

      def active?
        return false unless @connection.open?
        # return true if @connection.transaction_started
        @connection.query("SELECT 1 FROM RDB$DATABASE")
        true
      rescue
        false
      end

      def reconnect!
        disconnect!
        @connection = ::Fb::Database.connect(@config)
      end

      def disconnect!
        super
        @connection.close rescue nil
      end

      def reset!
        reconnect!
      end

      def requires_reloading?
        false
      end

      def create_savepoint(name = current_savepoint_name)
        execute("SAVEPOINT #{name}")
      end

      def rollback_to_savepoint(name = current_savepoint_name)
        execute("ROLLBACK TO SAVEPOINT #{name}")
      end

      def release_savepoint(name = current_savepoint_name)
        execute("RELEASE SAVEPOINT #{name}")
      end

      protected

      def initialize_type_map(m)
        super
        m.register_type %r(timestamp)i, Type::DateTime.new
        m.alias_type %r(blob sub_type text)i, 'text'
      end

      def translate_exception(e, message)
        case e.message
          when /violation of FOREIGN KEY constraint/
            InvalidForeignKey.new(message)
          when /violation of PRIMARY or UNIQUE KEY constraint/, /attempt to store duplicate value/
            RecordNotUnique.new(message)
          when /This operation is not defined for system tables/
            ActiveRecordError.new(message)
          else
            super
        end
      end

    end
  end
end