module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module SchemaStatements

        def native_database_types
          @native_database_types ||= initialize_native_database_types.freeze
        end

        private

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

      end
    end
  end
end