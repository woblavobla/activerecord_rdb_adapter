require 'active_record/tasks/database_tasks'

module ActiveRecord
  module Tasks

    class RdbDatabaseTasks
      delegate :rdb_connection_config, :establish_connection, :to => ::ActiveRecord::Base
    end

  end
end