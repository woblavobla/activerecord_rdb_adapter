module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module DatabaseStatements

        # Returns an array of arrays containing the field values.
        # Order is the same as that returned by +columns+.
        def select_rows(sql, name = nil, binds = [])
          exec_query(sql, name, binds).to_a.map(&:values)
        end

        def select_all(*)
          super
        end

        def execute(sql, name = nil)
          translate_and_log(sql, [], name) do |args|
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              @connection.execute(*args)
            end
          end
        end

        def execute_with_args(*args)
          ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
            @connection.execute(*args)
          end
        end

        def execute_and_free(*args)
          yield execute_with_args(*args)
        end

        # Executes +sql+ statement in the context of this connection using
        # +binds+ as the bind substitutes. +name+ is logged along with
        # the executed +sql+ statement.
        def exec_query(sql, name = 'SQL', binds = [])
          translate_and_log(sql, binds, name) do |args|
            result, rows = execute_and_free(*args) do |cursor|
              [cursor.fields, cursor.fetchall]
            end
            next result unless result.respond_to?(:map)
            cols = result.map {|col| col.name.freeze}
            ActiveRecord::Result.new(cols, rows)
          end
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
              read_committed: "READ COMMITTED",
              repeatable_read: "REPEATABLE READ",
              serializable: "SERIALIZABLE"
          }
        end

        # Allows providing the :transaction option to ActiveRecord::Base.transaction
        # in 4.0.2+. Can accept verbatim isolation options like 'WAIT READ COMMITTED'
        def begin_isolated_db_transaction(isolation)
          @connection.transaction transaction_isolation_levels.fetch(isolation, isolation)
        end

        # Commits the transaction (and turns on auto-committing).
        def commit_db_transaction
          log('commit transaction', nil) {@connection.commit}
        end

        # Rolls back the transaction (and turns on auto-committing). Must be
        # done if the transaction block raises an exception or returns false.
        def rollback_db_transaction
          log('rollback transaction', nil) {@connection.rollback}
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

        # Returns an array of record hashes with the column names as keys and
        # column values as values. ActiveRecord >= 4 returns an ActiveRecord::Result.
        def select(sql, name = nil, binds = [])
          exec_query(sql, name, binds)
        end

        def last_inserted_id(_result)
          nil
        end

        private

        def translate_and_log(sql, binds = [], name = nil)
          values = binds.map {|bind| type_cast(bind.value, bind)}

          if sql =~ /(CREATE TABLE|ALTER TABLE)/
            sql.dup.gsub!(/(\@BINDDATE|BINDDATE\@)/m, '\'')
          else
            if sql.is_a?(String)
              sql.dup.gsub!(/\@BINDBINARY(.*?)BINDBINARY\@/m) do |extract|
                values << decode(extract[11...-11]) and '?'
              end

              sql.dup.gsub!(/\@BINDDATE(.*?)BINDDATE\@/m) do |extract|
                values << extract[9...-9] and '?'
              end
            else
              sql = sql.to_sql
            end
          end

          log(sql, name, binds, values) {yield [sql, *values]}
        end
      end
    end
  end
end