require 'helper'

class TestDatabase < Test::Unit::TestCase
  def test_01_connect 
    dbh = new_database 
    assert(dbh)
    assert_kind_of(RDBI::Driver::SQLite3::Database, dbh)
    assert_kind_of(RDBI::Database, dbh)
    assert_equal(dbh.database_name, ":memory:")
  end

  def test_02_ping
    assert_equal(0, RDBI.ping(:SQLite3, :database => ":memory:"))
    assert_equal(0, new_database.ping)
  end
end
