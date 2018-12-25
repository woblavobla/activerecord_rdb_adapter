module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module DatabaseStatements # :nodoc:
        def execute(sql, name = nil)
          log(sql, name) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              @connection.query(sql)
            end
          end
        end

        def exec_query(sql, name = 'SQL', binds = [], prepare: false)
          type_casted_binds = type_casted_binds(binds)

          log(sql, name, binds, type_casted_binds) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              result = @connection.execute(sql, *type_casted_binds)
              if result.is_a?(Fb::Cursor)
                fields = result.fields.map(&:name)
                rows = result.fetchall.map do |row|
                  row.map do |col|
                    col.encode('UTF-8', @connection.encoding)
                  rescue StandardError
                    col
                  end
                end
                result.close
                ActiveRecord::Result.new(fields, rows)
              else
                result
              end
            end
          end
        rescue StandardError => e
          raise e.message.encode('UTF-8', @connection.encoding)
        end

        def explain(arel, binds = [])
          to_sql(arel, binds)
        end

        # Begins the transaction (and turns off auto-committing).
        def begin_db_transaction
          log('begin transaction', nil) do
            begin_isolated_db_transaction(default_transaction_isolation)
          end
        end

        # Default isolation levels for transactions. This method exists
        # in 4.0.2+, so it's here for backward compatibility with AR 3
        def transaction_isolation_levels
          {
            read_committed: 'READ COMMITTED',
            repeatable_read: 'REPEATABLE READ',
            serializable: 'SERIALIZABLE'
          }
        end

        # Allows providing the :transaction option to ActiveRecord::Base.transaction
        # in 4.0.2+. Can accept verbatim isolation options like 'WAIT READ COMMITTED'
        def begin_isolated_db_transaction(isolation)
          @connection.transaction transaction_isolation_levels.fetch(isolation, isolation)
        end

        # Commits the transaction (and turns on auto-committing).
        def commit_db_transaction
          log('commit transaction', nil) { @connection.commit }
        end

        # Rolls back the transaction (and turns on auto-committing). Must be
        # done if the transaction block raises an exception or returns false.
        def rollback_db_transaction
          log('rollback transaction', nil) { @connection.rollback }
        end

        def default_sequence_name(table_name, _column = nil)
          "#{table_name.to_s.tr('-', '_')[0, table_name_length - 4]}_seq"
        end

        # Set the sequence to the max value of the table's column.
        def reset_sequence!(table, column, sequence = nil)
          sequence ||= default_sequence_name(table, column)
          max_id = select_value("select max(#{column}) from #{table}")
          execute("alter sequence #{sequence} restart with #{max_id}")
        end

        # Uses the raw connection to get the next sequence value.
        def next_sequence_value(sequence_name)
          @connection.query("SELECT NEXT VALUE FOR #{sequence_name} FROM RDB$DATABASE")[0][0]
        end

        def last_inserted_id(_result)
          nil
        end
      end
    end
  end
end
