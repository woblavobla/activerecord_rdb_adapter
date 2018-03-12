module ActiveRecord
  module ConnectionAdapters
    module Rdb
      class SchemaCreation < AbstractAdapter::SchemaCreation

        private

        def visit_TableDefinition(o)
          create_sql = "CREATE#{' TEMPORARY' if o.temporary} TABLE #{quote_table_name(o.name)} "

          statements = o.columns.map { |c| accept c }
          statements << accept(o.primary_keys) if o.primary_keys

          if supports_indexes_in_create?
            statements.concat(o.indexes.map { |column_name, options| index_in_create(o.name, column_name, options) })
          end

          if supports_foreign_keys_in_create?
            statements.concat(o.foreign_keys.map { |to_table, options| foreign_key_in_create(o.name, to_table, options) })
          end

          create_sql << "(#{statements.join(', ')})" if statements.present?
          add_table_options!(create_sql, table_options(o))
          create_sql << " AS #{@conn.to_sql(o.as)}" if o.as
          create_sql
        end

      end
    end
  end
end