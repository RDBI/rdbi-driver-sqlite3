require 'rubygems'
gem 'test-unit'
require 'test/unit'

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rdbi'
require 'rdbi/driver/sqlite3'

class Test::Unit::TestCase

  SQL = [
    'create table foo (integer bar)',
  ]

  def new_database
    RDBI.connect(:SQLite3, :database => ":memory:")
  end

  def init_database
    dbh = new_database
    SQL.each { |query| dbh.execute(query) }
    return dbh
  end

end
