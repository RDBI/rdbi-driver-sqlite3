require 'helper'

class TestDatabase < Test::Unit::TestCase
  def test_01_works
    dbh = new_database 
    assert(dbh)
    assert_kind_of(RDBI::Driver::SQLite3::Database, dbh)
    assert_kind_of(RDBI::Database, dbh)
    assert_equal(dbh.database_name, ":memory:")
  end
end
