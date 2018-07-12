module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module ColumnDumper
        def column_spec_for_primary_key(column)
          return {} if default_primary_key?(column)
          spec = { id: schema_type(column).inspect }
          spec.merge!(prepare_column_options(column))#.except!(:null))
          spec[:default] ||= explicit_primary_key_default?(column)
          spec
        end

        private
        def schema_type(column)
          if column.bigint?
            :bigint
          else
            column.type.type
          end
        end
      end

      module SchemaDumper

      end

    end
  end
end