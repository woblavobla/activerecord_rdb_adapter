module ActiveRecord
  module ConnectionAdapters
    class RdbColumn < Column # :nodoc:
      class << self
        def sql_type_for(field)
          sql_type = field[:sql_type]
          sub_type = field[:sql_subtype]

          sql_type << case sql_type
                      when /(numeric|decimal)/i
                        "(#{field[:precision]},#{field[:scale].abs})"
                      when /(int|float|double|char|varchar|bigint)/i
                        "(#{field[:length]})"
                      else
                        ''
                      end

          sql_type << ' sub_type text' if /blob/i.match?(sql_type) && sub_type == 1
          sql_type
        end
      end

      attr_reader :sub_type, :domain

      def initialize(name, default, sql_type_metadata = nil, null = true, table_name = nil, rdb_options = {})
        @domain, @sub_type = rdb_options.values_at(:domain, :sub_type)
        name = name.dup
        name.downcase!
        super(name, parse_default(default), sql_type_metadata, null, table_name)
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
        return if default.nil? || /null/i.match?(default)
        d = default.dup
        d.gsub!(/^\s*DEFAULT\s+/i, '')
        d.gsub!(/(^'|'$)/, '')
        d
      end

      def simplified_type(field_type)
        return :datetime if /timestamp/i.match?(field_type)
        return :text if /blob sub_type text/i.match?(field_type)
        super
      end
    end
  end
end
