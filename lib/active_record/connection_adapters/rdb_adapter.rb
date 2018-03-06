require 'base64'
require 'active_record'

module ActiveRecord
  module ConnectionAdapters
    class RdbAdapter < AbstractAdapter
      include Rdb::DatabaseStatements
      include Rdb::Quoting

      ADAPTER_NAME = 'Rdb'.freeze

      def initialize(connection, logger = nil, config = {})
        super(connection, logger, config)
        # Our Responsibility
        @connection_options = config
        connect
      end

      def arel_visitor
        Arel::Visitors::Rdb.new self
      end

      def valid_type?(type)
        !native_database_types[type].nil?
      end

    end
  end
end