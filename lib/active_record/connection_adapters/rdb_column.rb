module ActiveRecord
  module ConnectionAdapters
    class RdbColumn < Column

      class << self

        def sql_type_for(field)
          type, sub_type, domain = field.values_at(:type, :sub_type, :domain)
          sql_type = ::Fb::SqlType.from_code(type, sub_type || 0).downcase

          case sql_type
            when /(numeric|decimal)/
              sql_type << "(#{field[:precision]},#{field[:scale].abs})"
            when /(int|float|double|char|varchar|bigint)/
              sql_type << "(#{field[:limit]})"
            else
              sql_type << ''
          end

          sql_type << ' sub_type text' if sql_type =~ /blob/ && sub_type == 1
          sql_type
        end
      end

      attr_reader :sub_type, :domain

      def initialize(name, default, sql_type_metadata = nil, null = true, table_name = nil, rdb_options = {})
        @domain, @sub_type = rdb_options.values_at(:domain, :sub_type)
        super(name.downcase, parse_default(default), sql_type_metadata, null, table_name)
      end

      def sql_type
        @sql_type_metadata[:sql_type]
      end

      def type
        @sql_type_metadata[:type]
      end

      def precision
        @sql_type_metadata[:precision]
      end

      def scale
        @sql_type_metadata[:scale]
      end

      def limit
        @sql_type_metadata[:limit]
      end

      private

      def parse_default(default)
        return if default.nil? || default =~ /null/i
        default.gsub(/^\s*DEFAULT\s+/i, '').gsub(/(^'|'$)/, '')
      end

      def simplified_type(field_type)
        return :datetime if field_type =~ /timestamp/
        return :text if field_type =~ /blob sub_type text/
        super
      end

    end
  end
end