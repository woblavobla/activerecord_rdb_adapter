module ActiveRecord
  module Tasks # :nodoc:
    class RdbDatabaseTasks # :nodoc:
      delegate :rdb_connection, :rdb_connection_config, :establish_connection, to: ::ActiveRecord::Base

      def initialize(configuration)
        @configuration = rdb_connection_config(configuration)
      end

      def create
        rdb_database.create
        # create_db
        establish_connection configuration
      rescue ::Fb::Error => e
        raise unless e.message.include?('File exists')
        raise DatabaseAlreadyExists
      end

      def drop
        establish_connection configuration
        rdb_database.drop
      rescue ::Fb::Error => e
        raise ::ActiveRecord::ConnectionNotEstablished, e.message
      end

      def purge
        begin
          drop
        rescue StandardError
          nil
        end
        create
      end

      def structure_dump(filename)
        isql :extract, output: filename
      end

      def structure_load(filename)
        isql input: filename
      end

      private

      def rdb_database
        ::Fb::Database.new(configuration)
      end

      # Executes isql commands to load/dump the schema.
      # The generated command might look like this:
      #   isql db/development.fdb -user SYSDBA -password masterkey -extract
      def isql(*args)
        opts = args.extract_options!
        user, pass = configuration.values_at(:username, :password)
        user ||= configuration[:user]
        opts.reverse_merge!(user: user, password: pass)
        cmd = [isql_executable, configuration[:database]]
        cmd += opts.map { |name, val| "-#{name} #{val}" }
        cmd += args.map { |flag| "-#{flag}" }
        cmd = cmd.join(' ')
        raise "Error running: #{cmd}" unless Kernel.system(cmd)
      end

      def isql_create(*_args)
        "#{isql_executable} -input "
      end

      # Finds the isql command line utility from the PATH
      # Many linux distros call this program isql-fb, instead of isql
      def isql_executable
        require 'mkmf'
        exe = %w[isql-fb isql].detect(&method(:find_executable0))
        exe || abort('Unable to find isql or isql-fb in your $PATH')
      end

      attr_reader :configuration

    end
  end
end
