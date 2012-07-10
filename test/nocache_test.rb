require 'minitest/autorun'
require File.expand_path('../test_helper', __FILE__)


class Hash
  def mget(str, default)
    parts = str.split(".")
    parts.inject(self) {|val, part|
      val && val[part]
    } || default
  end
end

module Vtocc
  class NocacheTest < GenericTestCase

    def execute(a, b = {})
      curs = @@conn.cursor
      curs.execute(a,b)
      curs
    end

    def conncection

    end

    def test_data
      cu = execute("select * from vtocc_test where intval=1")
      assert_equal(cu.description, [['intval', 3], ['floatval', 4], ['charval', 253], ['binval', 253]])
      assert_equal(cu.rowcount, 1)
      assert_equal(cu.fetchone, [1, 1.12345, "\xc2\xa2", "\x00\xff"])
      cu = execute("select * from vtocc_test where intval=2")
      assert_equal(cu.fetchone, [2, None, '', None])
    end

    def test_binary
      execute("begin")
      binary_data = '\x00\'\"\b\n\r\t\x1a\\\x00\x0f\xf0\xff'
      execute("insert into vtocc_test values(4, null, null, '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\\x00\x0f\xf0\xff')")
      execute("insert into vtocc_test values(5, null, null, ?)", binary_data)
      execute("commit")
      cu = execute("select * from vtocc_test where intval=4")
      assert_equal(cu.fetchone[3], binary_data)
      cu = execute("select * from vtocc_test where intval=5")
      assert_equal(cu.fetchone[3], binary_data)
      execute("begin")
      execute("delete from vtocc_test where intval in (4,5)")
      execute("commit")
    end

    def test_simple_read
      vstart = debug_vars
      cu = execute("select * from vtocc_test limit 2")
      vend = debug_vars
      assert_equal(cu.rowcount, 2)
      assert_equal(vstart.mget("Queries.TotalCount", 0)+1, vend.Queries.TotalCount)
      assert_equal(vstart.mget("Queries.Histograms.PASS_SELECT.Count", 0)+1, vend.Queries.Histograms.PASS_SELECT.Count)
      assert_not_equal(vend.mget("Voltron.ConnPool.Size", 0), 0)
    end

    def test_commit
      vstart = debug_vars
      execute("begin")
      assert_not_equal(@@conn.transaction_id, 0)
      execute("insert into vtocc_test (intval, floatval, charval, binval) values(4, null, null, null)")
      execute("commit")
      cu = execute("select * from vtocc_test")
      assert_equal(cu.rowcount, 4)
      execute("begin")
      execute("delete from vtocc_test where intval=4")
      execute("commit")
      cu = execute("select * from vtocc_test")
      assert_equal(cu.rowcount, 3)
      vend = debug_vars
      # We should have at least one connection
      assert_not_equal(vend.mget("Voltron.TxPool.Size", 0), 0)
      assert_equal(vstart.mget("Transactions.TotalCount", 0)+2, vend.Transactions.TotalCount)
      assert_equal(vstart.mget("Transactions.Histograms.Completed.Count", 0)+2, vend.Transactions.Histograms.Completed.Count)
      assert_equal(vstart.mget("Queries.TotalCount", 0)+4, vend.Queries.TotalCount)
      assert_equal(vstart.mget("Queries.Histograms.PLAN_INSERT_PK.Count", 0)+1, vend.Queries.Histograms.PLAN_INSERT_PK.Count)
      assert_equal(vstart.mget("Queries.Histograms.DML_PK.Count", 0)+1, vend.Queries.Histograms.DML_PK.Count)
      assert_equal(vstart.mget("Queries.Histograms.PASS_SELECT.Count", 0)+2, vend.Queries.Histograms.PASS_SELECT.Count)
    end

    def test_integrity_error
      vstart = debug_vars
      execute("begin")
      begin
        execute("insert into vtocc_test values(1, null, null, null)")
      rescue Mysql2::Error, OperationalError => e
        assert_equal(e[0], 1062)
        assert_match(e.message, /Duplicate/)
      else
        assert(false,"Did not receive exception")
      ensure
        execute("rollback")
      end
      vend = debug_vars
      assert_equal(vstart.mget("Errors.DupKey", 0)+1, vend.Errors.DupKey)
    end

    def test_rollback
      vstart = debug_vars
      execute("begin")
      assert_not_equal(@@conn.transaction_id, 0)
      execute("insert into vtocc_test values(4, null, null, null)")
      execute("rollback")
      cu = execute("select * from vtocc_test")
      assert_equal(cu.rowcount, 3)
      vend = debug_vars
      assert_not_equal(vend.mget("Voltron.TxPool.Size", 0), 0)
      assert_equal(vstart.mget("Transactions.TotalCount", 0)+1, vend.Transactions.TotalCount)
      assert_equal(vstart.mget("Transactions.Histograms.Aborted.Count", 0)+1, vend.Transactions.Histograms.Aborted.Count)
    end

    def test_nontx_dml
      vstart = debug_vars
      begin
        execute("insert into vtocc_test values(4, null, null, null)")
      rescue Mysql2::Error, OperationalError => e
        assert_match(e.message, /DMLs/)
      else
        assert(false,"Did not receive exception")
      end
      vend = debug_vars
      assert_equal(vstart.mget("Errors.Fail", 0)+1, vend.Errors.Fail)
    end

    def test_trailing_comment
      vstart = debug_vars
      execute("select * from vtocc_test where intval=?", 1)
      vend = debug_vars
      assert_equal(vstart.mget("Voltron.QueryCache.Length", 0)+1, vend.mget("Voltron.QueryCache.Length", 0))
      # This should not increase the query cache size
      execute("select * from vtocc_test where intval=? /* trailing comment */", 1)
      vend = debug_vars
      assert_equal(vstart.mget("Voltron.QueryCache.Length", 0)+1, vend.mget("Voltron.QueryCache.Length", 0))
    end

    def test_for_update
      begin
        execute("select * from vtocc_test where intval=2 for update")
      rescue Mysql2::Error, OperationalError => e
        assert_match(e.message, /Disallowed/)
      else
        assert(false,"Did not receive exception")
      end

      # If these throw no exceptions, we're good
      execute("begin")
      execute("select * from vtocc_test where intval=2 for update")
      execute("commit")
      # Make sure the row is not locked for read
      execute("select * from vtocc_test where intval=2")
    end

    def test_pool_size
      vstart = debug_vars
      execute("set vt_pool_size=1")
      begin
        execute("select sleep(3) from dual")
      rescue Mysql2::Error, OperationalError => e
        pass
      else
        assert(false,"Did not receive exception")
      end
      execute("select 1 from dual")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.ConnPool.Capacity", 0), 1)
      assert_equal(vstart.mget("Voltron.ConnPool.WaitCount", 0)+1, vend.mget("Voltron.ConnPool.WaitCount"))
      execute("set vt_pool_size=16")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.ConnPool.Capacity", 0), 16)
    end

    def test_transaction_cap
      vstart = debug_vars
      execute("set vt_transaction_cap=1")
      co2 = self.class.connect
      execute("begin")
      begin
        cu2 = co2.cursor
        cu2.execute("begin", {})
      rescue Mysql2::Error, OperationalError => e
        assert_match(e.message, /Transaction/)
      else
        assert(false,"Did not receive exception")
      ensure
        cu2.close
        co2.close
      end
      execute("commit")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.TxPool.Capacity"), 1)
      execute("set vt_transaction_cap=20")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.TxPool.Capacity",0), 20)
    end

=begin
    def test_transaction_timeout
      vstart = debug_vars
      execute("set vt_transaction_timeout=1")
      execute("begin")
      sleep(2)
      begin
        execute("commit")
      rescue Mysql2::Error, OperationalError => e
        assert_match(e.message, /Transaction/)
      else
        assert(false,"Did not receive exception")
      end
      vend = debug_vars
      assert_equal(vend.mget("Voltron.ActiveTxPool.Timeout",0), 1)
      assert_equal(vstart.mget("Kills.Transactions", 0)+1, vend.Kills.Transactions)
      execute("set vt_transaction_timeout=30")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.ActiveTxPool.Timeout",0), 30)
    end
=end

    def test_query_cache
      vstart = debug_vars
      execute("set vt_query_cache_size=1")
      execute("select * from vtocc_test where intval=?", 1)
      execute("select * from vtocc_test where intval=?", 1)
      vend = debug_vars
      assert_equal(vend.mget("Voltron.QueryCache.Length",0), 1)
      assert_equal(vend.mget("Voltron.QueryCache.Size",0), 1)
      assert_equal(vend.mget("Voltron.QueryCache.Capacity",0), 1)
      execute("set vt_query_cache_size=5000")
      execute("select * from vtocc_test where intval=?", 1)
      vend = debug_vars
      assert_equal(vend.mget("Voltron.QueryCache.Length",0), 2)
      assert_equal(vend.mget("Voltron.QueryCache.Size",0), 2)
      assert_equal(vend.mget("Voltron.QueryCache.Capacity",0), 5000)
    end

    def test_schema_reload_time
      mcu = @@mysql_conn
      mcu.query("create table vtocc_temp(intval int)")
      # This should cause a reload
      execute("set vt_schema_reload_time=600")
      begin
        1.upto(10).each do |i|
          begin
            execute("select * from vtocc_temp")
          rescue Mysql2::Error => e
            assert_match(e.message, /not found in schema/)
            sleep(1)
          else
            break
          end
        end
        # Should not throw an exception
        execute("select * from vtocc_temp")
      ensure
        mcu.execute("drop table vtocc_temp")
        mcu.close
      end
    end

    def test_max_result_size
      execute("set vt_max_result_size=2")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.MaxResultSize",0), 2)
      begin
        execute("select * from vtocc_test")
      rescue Mysql2::Error, OperationalError => e
        assert_match(e.message, /Row/)
      else
        assert(false,"Did not receive exception")
      end
      execute("set vt_max_result_size=10000")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.MaxResultSize",0), 10000)
    end

    def test_query_timeout
      vstart = debug_vars
      conn = Vtocc::VtoccConnection.connect("localhost:9461", 5, dbname: @@cfg[:database])
      cu = conn.cursor
      execute("set vt_query_timeout=1")
      begin
        cu.execute("begin", {})
        cu.execute("select sleep(2) from vtocc_test", {})
      rescue Mysql2::Error, OperationalError => e
        if e.message !~ /Query/ && e.message !~ /error: Lost connection/
          print e.message
          assert(false,"Query not killed as expected")
        end
      else
        assert(false,"Did not receive exception")
      end

      begin
        cu.execute("select 1 from dual", {})
      rescue Mysql2::Error, OperationalError => e
        assert_match(e.message, /Transaction/)
      else
        assert(false,"Did not receive exception")
      end

      begin
        cu.close
        conn.close
      rescue Mysql2::Error, OperationalError => e
        assert_match(str(e), /Transaction/)
      else
        assert(false,"Did not receive exception")
      end

      vend = debug_vars
      assert_equal(vend.mget("Voltron.ActivePool.Timeout",0), 1)
      assert_equal(vstart.mget("Kills.Queries", 0)+1, vend.Kills.Queries)
      execute("set vt_query_timeout=30")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.ActivePool.Timeout",0), 30)
    end

    def test_idle_timeout
      vstart = debug_vars
      execute("set vt_idle_timeout=1")
      sleep(2)
      execute("select 1 from dual")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.ConnPool.IdleTimeout",0), 1)
      assert_equal(vend.mget("Voltron.TxPool.IdleTimeout",0), 1)
      execute("set vt_idle_timeout=1800")
      vend = debug_vars
      assert_equal(vend.mget("Voltron.ConnPool.IdleTimeout",0), 1800)
      assert_equal(vend.mget("Voltron.TxPool.IdleTimeout",0), 1800)
    end

    def test_consolidation
      vstart = debug_vars
      1.upto(2).each do |i|
        begin
          execute("select sleep(3) from dual")
        rescue Mysql2::Error, OperationalError => e
          pass
        end
      end
      vend = debug_vars
      assert_equal(vstart.mget("Waits.TotalCount", 0)+1, vend.mget("Waits.TotalCount", 0))
      assert_equal(vstart.mget("Waits.Histograms.Consolidations.Count", 0)+1, vend.mget("Waits.Histograms.Consolidations.Count", 0))
    end

    def test_sqls
      error_count = @env.run_cases(nocache_cases.nocache_cases)
      if error_count != 0
        assert(false,"test_execution errors: #{error_count}")
      end
    end
  end
end
