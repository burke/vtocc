require 'vtocc'

module Vtocc

  class TabletConnection

    attr_writer :session_id
    def initialize(addr, dbname, timeout)
      @addr = addr
      @dbname = dbname
      @timeout = timeout
      @client = BsonRpcClient.new(uri, timeout)
    end

    def dial
      @client.close if @client
      @transaction_id = 0
      @session_id = 0
    end

    def close
      rollback
      @client.close
    end
    # TODO -- we could do a finalizer here to mirror python better...

    def cursor(klass = TabletCursor, *a)
      klass.new(self, *a)
    end

    def begin
      if @transaction_id && @transaction_id != 0
        raise NotImplementedError, "Nested transactions not supported"
      end
      req = make_req
      begin
        response = @client.call('SqlQuery.Begin', req)
        @transaction_id = response.reply
      rescue GoRpcError => e
        raise OperationalError, e.message
      end
    end

    def commit
      return unless @transaction_id

      req = make_req
      # NOTE(msolomon) Unset the transaction_id irrespective of the RPC's
      # response. The intent of commit is that no more statements can be made on
      # this transaction, so we guarantee that. Transient errors between the
      # db and the client shouldn't affect this part of the bookkeeping.
      # Do this after fill_session, since this is a critical part.
      @transaction_id = 0
      begin
        response = @client.call('SqlQuery.Commit', req)
        return response.reply
      rescue GoRpcError => e
        raise OperationalError, e.message
      end
    end

    def rollback
      return unless @transaction_id

      req = make_req
      # NOTE(msolomon) Unset the transaction_id irrespective of the RPC. If the
      # RPC fails, the client will still choose a new transaction_id next time
      # and the tablet server will eventually kill the abandoned transaction on
      # the server side.
      @transaction_id = 0
        response = @client.call('SqlQuery.Rollback', req)
        return response.reply
      begin
      rescue GoRpcError
        raise OperationalError, e.message
      end
    end

    def convert_bind_vars(a); a; end

    # in general, you don't really want to call this.
    # you should interface through the cursor.
    def _execute(sql, bind_variables)
      binds = convert_bind_vars(bind_variables) # TODO
      req = make_req
      req['Sql'] = sql
      req['BindVariables'] = binds

      fields = []
      conversions = []
      results = []
      begin
        response = @client.call('SqlQuery.Execute', req)
        reply = response.reply

        reply['Fields'].each do |field|
          fields << [field['Name'].to_s, field['Type'].to_s]
          # conversions.append(TODO) # TODO WTF
        end

        reply['Rows'].each do |row|
          results << make_row(row, conversions)
        end

        rowcount = reply['RowsAffected']
        lastrowid = reply['InsertId']
      rescue GoRpcError => e
        raise OperationalError, e.message
      #rescue
        # log low-level error TODO
      end
      [results, rowcount, lastrowid, fields]
    end

    private

    def make_row(row, conversions)
      converted_row = []
      # TODO all this
      row
    end

    def make_req
      {
        'TransactionId' => @transaction_id,
        'ConnectionId'  => 0,
        'SessionId'     => @session_id
      }
    end

    def uri
      "http://#{@addr}/_bson_rpc_"
    end

  end

end
