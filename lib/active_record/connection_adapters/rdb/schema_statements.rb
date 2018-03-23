module ActiveRecord
  module ConnectionAdapters
    module Rdb
      module SchemaStatements

        def native_database_types
          @native_database_types ||= initialize_native_database_types.freeze
        end

        def tables(_name = nil)
          @connection.table_names
        end

        def views
          @connection.view_names
        end

        # Returns an array of indexes for the given table.
        def indexes(table_name, _name = nil)
          @connection.indexes.values.map {|ix|
            if ix.table_name == table_name.to_s && ix.index_name !~ /^rdb\$/
              IndexDefinition.new(table_name, ix.index_name, ix.unique, ix.columns)
            end
          }.compact
        end

        def primary_key(table_name) #:nodoc:
          row = @connection.query(<<-end_sql)
            SELECT s.rdb$field_name
            FROM rdb$indices i
            JOIN rdb$index_segments s ON i.rdb$index_name = s.rdb$index_name
            LEFT JOIN rdb$relation_constraints c ON i.rdb$index_name = c.rdb$index_name
            WHERE i.rdb$relation_name = '#{ar_to_rdb_case(table_name)}'
            AND c.rdb$constraint_type = 'PRIMARY KEY';
          end_sql

          row.first && rdb_to_ar_case(row.first[0].rstrip)
        end

        def columns(table_name, _name = nil)
          column_definitions(table_name).map do |field|
            field.symbolize_keys!.each {|k, v| v.rstrip! if v.is_a?(String)}
            sql_type_metadata = column_type_for(field)
            rdb_opt = field.slice(:domain, :sub_type)
            RdbColumn.new(field[:name], field[:default_source], sql_type_metadata, !field[:null_flag], table_name, rdb_opt)
          end
        end

        def create_table(name, options = {}) # :nodoc:
          if options.key? :temporary
            fail ActiveRecordError, 'Firebird does not support temporary tables'
          end

          if options.key? :as
            fail ActiveRecordError, 'Firebird does not support creating tables with a select'
          end

          if options.key? :force
            drop_table name, {:if_exists => true}
          end

          needs_sequence = options[:id]

          super name, options do |table_def|
            yield table_def if block_given?
            needs_sequence ||= table_def.needs_sequence
          end

          #commit_db_transaction

          return if options[:sequence] == false || !needs_sequence
          create_sequence(options[:sequence] || default_sequence_name(name))
          trg_sql = <<-END_SQL
            CREATE TRIGGER N$#{name.upcase} FOR #{name.upcase}
            ACTIVE BEFORE INSERT
            AS
            declare variable gen_val bigint;
            BEGIN
              if (new.ID is null) then
                new.ID = next value for #{options[:sequence] || default_sequence_name(name)};
              else begin
                gen_val = gen_id(#{options[:sequence] || default_sequence_name(name)}, 1);
                if (new.ID > gen_val) then
                  gen_val = gen_id(#{options[:sequence] || default_sequence_name(name)}, new.ID - gen_val);
              end
            END
          END_SQL
          execute(trg_sql)
          #commit_db_transaction
        end

        def drop_table(name, options = {}) # :nodoc:
          drop_sql = "DROP TABLE #{quote_table_name(name)}"
          if options[:if_exists]
            drop = !execute(squish_sql(<<-end_sql))
            select 1 from rdb$relations where rdb$relation_name = #{quote_table_name(name).gsub(/"/, '\'')}
            end_sql
                       .fetchall.empty?
          end

          trigger_name = "N$#{name.upcase}"
          drop_trigger(trigger_name) if trigger_exists?(trigger_name)

          sequence_name = options[:sequence] || default_sequence_name(name)
          drop_sequence(sequence_name) if sequence_exists?(sequence_name)

          execute(drop_sql) if drop
        end

        def create_sequence(sequence_name)
          execute("CREATE SEQUENCE #{sequence_name}") rescue nil
        end

        def drop_sequence(sequence_name)
          execute("DROP SEQUENCE #{sequence_name}") rescue nil
        end

        def drop_trigger(trigger_name)
          execute("DROP TRIGGER #{trigger_name}") rescue nil
        end

        def trigger_exists?(trigger_name)
          execute(squish_sql(<<-end_sql))
            select 1
            from rdb$triggers
             where rdb$trigger_name = '#{trigger_name}'
          end_sql
              .fetchall.size > 0
        end

        def add_column(table_name, column_name, type, options = {})
          super

          if type == :primary_key && options[:sequence] != false
            create_sequence(options[:sequence] || default_sequence_name(table_name))
          end

          return unless options[:position]
          # position is 1-based but add 1 to skip id column
          execute(squish_sql(<<-end_sql))
            ALTER TABLE #{quote_table_name(table_name)}
            ALTER COLUMN #{quote_column_name(column_name)}
            POSITION #{options[:position] + 1}
          end_sql
        end

        def remove_column(table_name, column_name, type = nil, options = {})
          indexes(table_name).each do |i|
            if i.columns.any? {|c| c == column_name.to_s}
              remove_index! i.table, i.name
            end
          end

          column_exist = execute(squish_sql(<<-END_SQL))
          select 1 from RDB$RELATION_FIELDS rf
            where lower(rf.RDB$RELATION_NAME) = '#{table_name.downcase}' and lower(rf.RDB$FIELD_NAME) = '#{column_name.downcase}'
          END_SQL
                             .fetchall.size > 0
          super if column_exist
        end

        def change_column(table_name, column_name, type, options = {})
          type_sql = type_to_sql(type, *options.values_at(:limit, :precision, :scale))

          if [:text, :string].include?(type)
            copy_column = 'c_temp'
            add_column table_name, copy_column, type, options
            execute(squish_sql(<<-end_sql))
            UPDATE #{table_name} SET #{copy_column} = #{column_name};
            end_sql
            remove_column table_name, column_name
            rename_column table_name, copy_column, column_name
          else
            execute(squish_sql(<<-end_sql))
            ALTER TABLE #{quote_table_name(table_name)}
            ALTER COLUMN #{quote_column_name(column_name)} TYPE #{type_sql}
            end_sql
          end
          change_column_null(table_name, column_name, !!options[:null]) if options.key?(:null)
          change_column_default(table_name, column_name, options[:default]) if options.key?(:default)


        end

        def change_column_default(table_name, column_name, default)
          execute(squish_sql(<<-end_sql))
            ALTER TABLE #{quote_table_name(table_name)}
            ALTER #{quote_column_name(column_name)}
            SET DEFAULT #{quote(default)}
          end_sql
        end

        def change_column_null(table_name, column_name, null, default = nil)
          change_column_default(table_name, column_name, default) if default

          execute(squish_sql(<<-end_sql))
            UPDATE RDB$RELATION_FIELDS
            SET RDB$NULL_FLAG=#{quote(null ? nil : 1)}
            WHERE RDB$FIELD_NAME='#{ar_to_fb_case(column_name)}'
            AND RDB$RELATION_NAME='#{ar_to_fb_case(table_name)}'
          end_sql
        end

        def rename_column(table_name, column_name, new_column_name)
          execute(squish_sql(<<-end_sql))
            ALTER TABLE #{quote_table_name(table_name)}
            ALTER #{quote_column_name(column_name)}
            TO #{quote_column_name(new_column_name)}
          end_sql

          rename_column_indexes(table_name, column_name, new_column_name)
        end

        def remove_index!(_table_name, index_name)
          execute "DROP INDEX #{quote_column_name(index_name)}"
        end

        def remove_index(table_name, options = {})
          index_name = index_name(table_name, options)
          execute "DROP INDEX #{quote_column_name(index_name)}"
        end

        def index_name(table_name, options) #:nodoc:
          if options.respond_to?(:keys) # legacy support
            if options[:column]
              index_name = "#{table_name}_#{Array.wrap(options[:column]) * '_'}"
              if index_name.length > 31
                "IDX_#{Digest::SHA256.hexdigest(index_name)[0..22]}"
              else
                index_name
              end
            elsif options[:name]
              options[:name]
            else
              fail ArgumentError, "You must specify the index name"
            end
          else
            index_name(table_name, :column => options)
          end
        end

        def type_to_sql(type, limit = nil, precision = nil, scale = nil, **args)
          if !args.nil? && !args.empty?
            limit = args[:limit] if limit == nil
            precision = args[:precision] if precision == nil
            scale = args[:scale] if scale == nil
          end
          case type
            when :integer
              integer_to_sql(limit)
            when :float
              float_to_sql(limit)
            when :text
              text_to_sql(limit)
            when :blob
              binary_to_sql(limit)
            when :string
              text_to_sql(limit)
            else
              type = type.to_sym if type
              if native = native_database_types[type]
                column_type_sql = (native.is_a?(Hash) ? native[:name] : native).dup

                if type == :decimal # ignore limit, use precision and scale
                  scale ||= native[:scale]

                  if precision ||= native[:precision]
                    if scale
                      column_type_sql << "(#{precision},#{scale})"
                    else
                      column_type_sql << "(#{precision})"
                    end
                  elsif scale
                    raise ArgumentError, "Error adding decimal column: precision cannot be empty if scale is specified"
                  end

                elsif [:datetime, :timestamp, :time, :interval].include?(type) && precision ||= native[:precision]
                  if (0..6) === precision
                    column_type_sql << "(#{precision})"
                  else
                    raise(ActiveRecordError, "No #{native[:name]} type has precision of #{precision}. The allowed range of precision is from 0 to 6")
                  end
                elsif (type != :primary_key) && (limit ||= native.is_a?(Hash) && native[:limit])
                  column_type_sql << "(#{limit})"
                end

                column_type_sql
              else
                type.to_s
              end
          end
        end

        private

        def column_definitions(table_name)
          exec_query(squish_sql(<<-end_sql), 'SCHEMA')
            SELECT
              r.rdb$field_name name,
              r.rdb$field_source "domain",
              f.rdb$field_type type,
              f.rdb$field_sub_type "sub_type",
              f.rdb$field_length "limit",
              f.rdb$field_precision "precision",
              f.rdb$field_scale "scale",
              COALESCE(r.rdb$default_source, f.rdb$default_source) default_source,
              COALESCE(r.rdb$null_flag, f.rdb$null_flag) null_flag
            FROM rdb$relation_fields r
            JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name
            WHERE r.rdb$relation_name = '#{ar_to_rdb_case(table_name)}'
            ORDER BY r.rdb$field_position
          end_sql
        end

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
           :boolean => {:name => 'boolean'}
          }
        end

        def column_type_for(field)
          sql_type = RdbColumn.sql_type_for(field)

          if ActiveRecord::VERSION::STRING < "4.2.0"
            [sql_type]
          else
            {:type => lookup_cast_type(sql_type), :sql_type => sql_type}
          end
        end

        def integer_to_sql(limit)
          return 'integer' if limit.nil?
          case limit
            when 1..2 then
              'smallint'
            when 3..4 then
              'integer'
            when 5..8 then
              'bigint'
            else
              fail ActiveRecordError, "No integer type has byte size #{limit}. "\
                                    "Use a NUMERIC with PRECISION 0 instead."
          end
        end

        def float_to_sql(limit)
          (limit.nil? || limit <= 4) ? 'float' : 'double precision'
        end

        def text_to_sql(limit)
          if limit && limit > 0
            "VARCHAR(#{limit})"
          else
            "VARCHAR(100)"
          end
        end

        def sequence_exists?(sequence_name)
          @connection.generator_names.include?(sequence_name)
        end

        def create_table_definition(*args)
          Rdb::TableDefinition.new(*args)
        end

        def squish_sql(sql)
          sql.strip.gsub(/\s+/, ' ')
        end

      end
    end
  end
end