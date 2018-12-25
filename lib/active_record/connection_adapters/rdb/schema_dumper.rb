module ActiveRecord
  module ConnectionAdapters
    module Rdb
      class SchemaDumper < ConnectionAdapters::SchemaDumper # :nodoc:
        private

        def column_spec_for_primary_key(column)
          spec = super
          spec.delete(:auto_increment) if column.type == :integer && column.auto_increment?
          spec
        end
      end
    end
  end
end
