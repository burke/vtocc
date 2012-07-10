require 'vtocc/tablet_connection'
require 'vtocc/query_reshaper'

module Vtocc
  class VtoccConnection < TabletConnection
    MAX_ATTEMPTS = 2

    def self.connect(addr, timeout, opts = {})
      dbname = opts.delete(:dbname)

      conn = new(addr, dbname, timeout)
      conn.dial
      conn
    end

    def alive?
      @client.call('OccManager.GetSessionId', @dbname) > 0
    rescue GoRpcError
      return false
    end


    def dial
      super
      begin
        response = @client.call('OccManager.GetSessionId', @dbname)
        @session_id = response.reply
      rescue GoRpcError => e
        raise OperationalError, e.message
      end
    end

    def _execute(*ary)
      sql, bind_variables = QueryReshaper.call(ary)

      super(sql, bind_variables)
    end

  end
end

