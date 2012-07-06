require "vtocc/version"
require 'bson'
require 'uri'
require 'socket'

module Vtocc

  class GoRpcError < StandardError; end
  class AppError < GoRpcError; end
  class TimeoutError < GoRpcError; end

  class GoRpcRequest
    attr_accessor :header, :body
    def initialize(header, args)
      @header = header
      @args = args
    end

    def sequence_id
      @header['Seq']
    end
  end

  class GoRpcResponse
    attr_accessor :header, :reply
    def error
      @header['Error']
    end

    def sequence_id
      @header['Seq']
    end
  end

  class GoRpcConn
    DEFAULT_READ_BUFFER_SIZE = 8192

    def initialize(timeout)
      @timeout = timeout
    end

    def dial(uri)
      uri = URI.parse(uri)
      # NOTE(msolomon) since the deadlines are approximate in the code, set
      # timeout to oversample to minimize waiting in the extreme failure mode.
      socket_timeout = @timeout / 10.0
      # TODO(burke) handle timeout. Ruby blows at this.
      @conn = TCPSocket.new(uri.host, uri.port)
      @conn.send("CONNECT #{uri.path} HTTP/1.0\n\n")

      loop do
        data = @conn.recv(1024)
        data or raise GoRpcError, "Unexpected EOF in handshake"
        return if data.index("\n\n")
      end
    end

    def close
      if @conn
        @conn.close
        @conn = nil
      end
    end

    def write_request(request_data)
      @start_time = Time.now
      @conn.send(request_data)
    end

    def read_response
      @start_time or raise GoRpcError, "no request pending"

      begin
        buf = []
        data, data_len = read_more(buf)

        # must read at least enough to get the length
        # 4 is the size of the BSON length
        while data_len < 4 # && !deadline_exceeded?
          data, data_len = read_more(buf)
        end

        # header_len is the size of the entire header including the length
        # add on an extra len_struct_size to get enough of the body to read size
        header_len = data[0..3].unpack("V")[0]

        while data_len < (header_len + 4) # && !deadline_exceeded?
          data, data_len = read_more(buf)
        end

        body_len = data[header_len..header_len+3].unpack("V")[0]
        total_len = header_len + body_len

        while data_len < total_len # && !deadline_exceeded?
          data, data_len = read_more(buf)
        end

        return data
      ensure
        @start_time = nil
      end
    end

    private

    def read_more(buf)
      begin
        data = @conn.recv(DEFAULT_READ_BUFFER_SIZE)
        data or raise Errno::EPIPE, "Unexpected EOF in read"
        # TODO rescue timeout
      end

      buf << data # DESTRUCTIVE. For consistency with python. Refactor once working maybe.
      if buf.size > 1
        data = buf.join('')
      end

      [data, data.length]
    end

  end

  class GoRpcClient

    def initialize(uri, timeout)
      @uri = uri
      @timeout = timeout
      @seq = 0
    end

    def conn
      @conn ||= GoRpcConn.new(@timeout).dial(@uri)
    end

    def close
      if @conn
        @conn.close
        @conn = nil
      end
    end

    def next_sequence_id
      @seq += 1
    end

    def encode_request(request)
      raise NotImplementedError
    end

    def decode_request(response, data)
      raise NotImplementedError
    end

    def call(method, request, response = nil)
      begin
        h = GoRpcClient.make_header(method, next_sequence_id)
        req = GoRpcRequest.new(h, request)
        @conn.write_request(encode_request(req))
        data = @conn.read_response
        @response ||= GoRpcResponse.new
        decode_response(@response, data)
        # TODO(burke) handle timeout
      rescue SocketError => e
        close
        raise GoRpcError, "#{e.message} method:#{method}"
      end

      if response.error
        raise AppError, "#{response.error}, method:#{method}"
      end
      if response.sequence_id != req.sequence_id
        close
        raise GoRpcError, "request sequence mismatch: #{response.sequence_id} != #{req.sequence_id} method:#{method}"
      end

      response
    end

    def self.make_header(method, sequence_id)
      {'ServiceMethod' => method, 'Seq' => sequence_id}
    end

  end


  class BsonRpcClient < GoRpcClient
    WRAPPED_FIELD = '_Val_'

    def encode_request(request)
      body = request.body
      Hash === request.body or body = {WRAPPED_FIELD: body}

      buf = BSON.serialize(request.header)
      buf.append!(BSON.serialize(body))
      buf.to_s
    rescue => e
      raise GoRpcError, "encode error"
    end

    def decode_response(response, data)
      offset, response.header = BSON.deserialize(data)
      index = data[0..3].unpack("V")[0]
      response.header = BSON.deserialize(data[0..index-1])
      response.reply = BSON.deserialize(data[index..-1])

      response.reply = response.reply[WRAPPED_FIELD] || response.reply
    rescue => e
      raise GoRpcError, "decode error"
    end
  end
end
