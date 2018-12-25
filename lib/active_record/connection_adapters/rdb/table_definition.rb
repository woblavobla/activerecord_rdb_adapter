module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module ColumnMethods # :nodoc:

        attr_accessor :needs_sequence

        def primary_key(name, type = :primary_key, **options)
          self.needs_sequence = true
          super
        end

      end

      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition # :nodoc:
        include ColumnMethods

        def new_column_definition(name, type, **options)
          super
        end
      end

      class Table < ActiveRecord::ConnectionAdapters::Table # :nodoc:
        include ColumnMethods
      end
    end
  end
end
