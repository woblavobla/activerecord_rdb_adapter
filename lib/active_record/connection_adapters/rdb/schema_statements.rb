module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module SchemaStatements
        def tables(_name = nil)
          @connection.table_names
        end

        def views
          @connection.view_names
        end

        def primary_key(table_name)
          row = @connection.query(<<-END_SQL)
            SELECT s.rdb$field_name
            FROM rdb$indices i
            JOIN rdb$index_segments s ON i.rdb$index_name = s.rdb$index_name
            LEFT JOIN rdb$relation_constraints c ON i.rdb$index_name = c.rdb$index_name
            WHERE i.rdb$relation_name = '#{ar_to_rdb_case(table_name)}'
            AND c.rdb$constraint_type = 'PRIMARY KEY';
          END_SQL

          row.first && rdb_to_ar_case(row.first[0].rstrip)
        end

        private

        def column_definitions(table_name)
          @connection.columns(table_name)
        end

        def new_column_from_field(table_name, field)
          type_metadata = fetch_type_metadata(field["sql_type"])
          ActiveRecord::ConnectionAdapters::Column.new(field["name"], field["default"], type_metadata, field["nullable"], table_name)
        end
      end
    end
  end
end