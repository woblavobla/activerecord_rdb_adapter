module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module SchemaStatements

        def native_database_types
          @native_database_types ||= initialize_native_database_types.freeze
        end

        def tables(_name = nil)
          @connection.table_names
        end

        def views
          rows = @connection.query(<<-end_sql)
             SELECT rdb$relation_name
             FROM rdb$relations
             WHERE rdb$view_blr IS NOT NULL
             AND (rdb$system_flag IS NULL OR rdb$system_flag = 0);
          end_sql
          rows.map {|row| row[0].strip}
        end

        # Returns an array of indexes for the given table.
        def indexes(table_name, _name = nil)
          @connection.indexes.values.map {|ix|
            if ix.table_name == table_name.to_s && ix.index_name !~ /^rdb\$/
              IndexDefinition.new(table_name, ix.index_name, ix.unique, ix.columns)
            end
          }.compact
        end

        def primary_key(table_name) #:nodoc:
          row = @connection.query(<<-end_sql)
            SELECT s.rdb$field_name
            FROM rdb$indices i
            JOIN rdb$index_segments s ON i.rdb$index_name = s.rdb$index_name
            LEFT JOIN rdb$relation_constraints c ON i.rdb$index_name = c.rdb$index_name
            WHERE i.rdb$relation_name = '#{ar_to_rdb_case(table_name)}'
            AND c.rdb$constraint_type = 'PRIMARY KEY';
          end_sql

          row.first && rdb_to_ar_case(row.first[0].rstrip)
        end

        def columns(table_name, _name = nil)
          column_definitions(table_name).map do |field|
            field.symbolize_keys!.each {|k, v| v.rstrip! if v.is_a?(String)}
            properties = field.values_at(:name, :default_source)
            properties += column_type_for(field)
            properties << !field[:null_flag]
            RdbColumn.new(*properties, field.slice(:domain, :sub_type))
          end
        end

        def column_type_for(field)
          sql_type = RdbColumn.sql_type_for(field)
          [lookup_cast_type(sql_type), sql_type]
        end

        private

        def column_definitions(table_name)
          exec_query(squish_sql(<<-end_sql), 'SCHEMA')
            SELECT
              r.rdb$field_name name,
              r.rdb$field_source "domain",
              f.rdb$field_type type,
              f.rdb$field_sub_type "sub_type",
              f.rdb$field_length "limit",
              f.rdb$field_precision "precision",
              f.rdb$field_scale "scale",
              COALESCE(r.rdb$default_source, f.rdb$default_source) default_source,
              COALESCE(r.rdb$null_flag, f.rdb$null_flag) null_flag
            FROM rdb$relation_fields r
            JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name
            WHERE r.rdb$relation_name = '#{ar_to_rdb_case(table_name)}'
            ORDER BY r.rdb$field_position
          end_sql
        end

        def initialize_native_database_types
          {:primary_key => 'integer not null primary key',
           :string => {:name => 'varchar', :limit => 255},
           :text => {:name => 'blob sub_type text'},
           :integer => {:name => 'integer'},
           :bigint => {:name => 'bigint'},
           :float => {:name => 'float'},
           :decimal => {:name => 'decimal'},
           :datetime => {:name => 'timestamp'},
           :timestamp => {:name => 'timestamp'},
           :time => {:name => 'time'},
           :date => {:name => 'date'},
           :binary => {:name => 'blob'},
           :boolean => {:name => boolean_domain[:name]}
          }
        end

        def create_table_definition(*args)
          Rdb::TableDefinition.new(*args)
        end

        def squish_sql(sql)
          sql.strip.gsub(/\s+/, ' ')
        end

      end
    end
  end
end