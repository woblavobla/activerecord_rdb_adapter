module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module ColumnMethods

        def primary_key(name, type = :primary_key, **options)
          super
        end

      end

      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition
        include ColumnMethods

        def new_column_definition(name, type, **options)
          super
        end
      end

      class Table < ActiveRecord::ConnectionAdapters::Table
        include ColumnMethods
      end
    end
  end
end