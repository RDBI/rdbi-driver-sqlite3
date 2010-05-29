require 'rubygems'
gem 'sqlite3-ruby'
require 'sqlite3'

db = SQLite3::Database.new ":memory:"
db.execute('create table fart (integer foo, varchar bar)')
p db.table_info('*')

__END__
SELECT name FROM sqlite_master 
WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'
UNION ALL 
SELECT name FROM sqlite_temp_master 
WHERE type IN ('table','view') 
ORDER BY 1
