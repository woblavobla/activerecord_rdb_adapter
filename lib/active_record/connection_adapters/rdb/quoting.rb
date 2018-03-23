module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module Quoting

        def quote_string(string) # :nodoc:
          string.gsub(/'/, "''")
        end

        def quoted_date(time)
          if time.is_a?(Time) || time.is_a?(DateTime)
            time.localtime.strftime("%d.%m.%Y %H:%M:%S")
          else
            time.strftime("%d.%m.%Y")
          end
        end

        def quote_column_name(column_name) # :nodoc:
          name = column_name.to_s.gsub(/(?<=[^\"\w]|^)position(?=[^\"\w]|$)/i, '"POSITION"').gsub(/(?<=[^\"\w]|^)value(?=[^\"\w]|$)/i, '"VALUE"')
          name = ar_to_rdb_case(name.to_s).gsub('"', '')
          @connection.dialect == 1 ? %Q(#{name}) : %Q("#{name}")
        end

        def quote_table_name_for_assignment(_table, attr)
          quote_column_name(attr)
        end

        def unquoted_true
          'TRUE'
        end

        def quoted_true # :nodoc:
          quote :true
        end

        def unquoted_false
          'FALSE'
        end

        def quoted_false # :nodoc:
          quote :false
        end

        def lookup_cast_type_from_column(column) # :nodoc:
          lookup_cast_type(column.try(:sql_type) || column.try(:type))
        end

        private
        def id_value_for_database(value)
          if primary_key = value.class.primary_key
            value.instance_variable_get(:@attributes)[primary_key].value_for_database
          end
        end

        def _quote(value)
          case value
            when Type::Binary::Data
              "@BINDBINARY#{Base64.encode64(value.to_s)}BINDBINARY@"
            when Time, DateTime
              "'#{value.strftime("%d.%m.%Y %H:%M")}'"
            when Date
              "'#{value.strftime("%d.%m.%Y")}'"
            else
              super
          end
        end

        def _type_cast(value)
          case value
            when Symbol, ActiveSupport::Multibyte::Chars, Type::Binary::Data
              value.to_s
            when Array
              value.to_yaml
            when Hash then
              encode_hash(value)
            when true then
              unquoted_true
            when false then
              unquoted_false
            # BigDecimals need to be put in a non-normalized form and quoted.
            when BigDecimal then
              value.to_s("F")
            when Type::Time::Value then
              quoted_time(value)
            when Date, Time, DateTime then
              quoted_date(value)
            when *types_which_need_no_typecasting
              value
            else
              raise TypeError
          end
        end

        def rdb_to_ar_case(column_name)
          column_name =~ /[[:lower:]]/ ? column_name : column_name.downcase
        end

        def ar_to_rdb_case(column_name)
          column_name =~ /[[:upper:]]/ ? column_name : column_name.upcase
        end

        def encode_hash(value)
          if value.is_a?(Hash)
            value.to_yaml
          else
            value
          end
        end

        if defined? Encoding
          def decode(s)
            Base64.decode64(s).force_encoding(@connection.encoding)
          end
        else
          def decode(s)
            Base64.decode64(s)
          end
        end
      end
    end
  end
end