require 'vtocc/tablet_connection'

module Vtocc
  class VtoccConnection < TabletConnection
    MAX_ATTEMPTS = 2

    def self.connect(addr, timeout, opts = {})
      dbname = opts.delete(:dbname)

      conn = new(addr, dbname, timeout)
      conn.dial
      conn
    end


    def dial
      super
      begin
        response = @client.call('OccManager.GetSessionId', @dbname)
        @session_id = response.reply
      rescue GoRpcError
        raise OperationalError
      end
    end

    private

  end
end

