module Vtocc
  class BaseCursor
    include Enumerable

    attr_reader :description, :rowcount
    def initialize(connection)
      @arraysize = 1
      @rowcount = 0
      @connection = connection
    end

    def close
      @connection = nil
      @results = nil
    end

    def execute(*args)
      @rowcount = 0
      @results = nil
      @description = nil
      @lastrowid = nil

      case args.first.strip.downcase
      when 'begin'    ; @connection.begin    ; return
      when 'commit'   ; @connection.commit   ; return
      when 'rollback' ; @connection.rollback ; return
      end

      resp = @connection._execute(*args)
      @results, @rowcount, @lastrowid, @description = resp
      @index = 0

      @rowcount
    end

    def fetchone
      @results or raise ProgrammingError, "fetch called before execute"
      return nil if @index >= @results.size

      @index += 1

      @results[@index - 1]
    end

    def fetchmany(size = nil)
      @results or raise ProgrammingError, "fetch called before execute"
      return [] if @index >= @results.size

      size ||= @arraysize

      res = @results[@index..@index+size-1]
      @index += size

      res
    end

    def fetchall
      @results or raise ProgrammingError, "fetch called before execute"
      fetchmany(@results.size - @index)
    end

    def callproc
      raise NotImplementedError
    end

    def executemany(*args)
      raise NotImplementedError
    end

    def nextset
      raise NotImplementedError
    end

    def setinputsizes(sizes)
    end

    def setoutputsize(size, column=nil)
    end

    def rownumber
      @index
    end

    def each
      until (val = fetchone).nil?
        yield val
      end
      nil
    end

  end

  class TabletCursor < BaseCursor
  end

end
