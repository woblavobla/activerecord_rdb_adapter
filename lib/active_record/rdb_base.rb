module ActiveRecord
  module ConnectionHandling # :nodoc:
    def rdb_connection(config)
      require 'fb'
      config = rdb_connection_config(config)
      db = ::Fb::Database.new(config)
      begin
        connection = db.connect
      rescue
        unless config[:create]
          require 'pp'
          pp config
          raise ConnectionNotEstablished, "No Firebird connections established."
        end
        connection = db.create.connect
      end
      ConnectionAdapters::RdbAdapter.new(connection, logger, config)
    end

    def rdb_connection_config(config)
      config = config.symbolize_keys.dup.reverse_merge(:downcase_names => true)
      port = config[:port] || 3050
      fail ArgumentError, 'No database specified. Missing argument: database.' unless config[:database]
      if config[:host].nil? || config[:host] =~ /localhost/i
        config[:database] = File.expand_path(config[:database], defined?(Rails) && Rails.root)
      end
      config[:database] = "#{config[:host]}/#{port}:#{config[:database]}" if config[:host]
      #config[:charset] = config[:charset].gsub(/-/, '') if config[:charset]
      #config[:encoding] = config[:encoding].gsub(/-/, '') if config[:encoding]
      config[:page_size] = 8192 unless config[:page_size]
      config[:readonly_selects] = true unless config[:readonly_selects].present?
      config
    end
  end
end