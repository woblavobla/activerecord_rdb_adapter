module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module DatabaseStatements

        def execute(sql, name = nil)
          super
        end

        def exec_query(sql, name = 'SQL', binds = [])
          super
        end
      end
    end
  end
end