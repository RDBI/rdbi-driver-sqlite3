require 'rdbi'
require 'sqlite3'

class RDBI::Driver::SQLite3 < RDBI::Driver
  def initialize(*args)
    super(Database, *args)
  end
end

class RDBI::Driver::SQLite3::Database < RDBI::Database
  def initialize(*args)
    super
    self.database_name = @connect_args[:database]
    @handle = ::SQLite3::Database.new(database_name)
    @handle.type_translation = false # XXX RDBI should handle this.
  end

  inline(:new_statement) { raise NoMethodError, "not done yet" }

  inline(:ping)     { 0 }
  inline(:rollback) { @handle.rollback; super() }
  inline(:commit)   { @handle.commit; super()   }
end
