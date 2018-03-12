require 'active_record/connection_adapters/rdb_adapter'

if defined?(::Rails::Railtie) && ::ActiveRecord::VERSION::MAJOR > 3
  class Railtie < ::Rails::Railtie
    rake_tasks do
      load 'active_record/tasks/rdb_database_tasks.rb'
      ActiveRecord::Tasks::DatabaseTasks.register_task(/rdb/, ActiveRecord::Tasks::RdbDatabaseTasks)
    end
  end
end