require 'rdbi'
require 'sqlite3'

class RDBI::Driver::SQLite3 < RDBI::Driver
  def initialize(*args)
    super(Database, *args)
  end
end

class RDBI::Driver::SQLite3 < RDBI::Driver
  class Database < RDBI::Database

    attr_accessor :handle

    def initialize(*args)
      super
      self.database_name = @connect_args[:database]
      @handle = ::SQLite3::Database.new(database_name)
      @handle.type_translation = false # XXX RDBI should handle this.
    end

    def disconnect
      @handle.close
      super
    end

    def transaction
      @handle.transaction
      super
    end

    def new_statement(query)
      Statement.new(query, self)
    end

    def preprocess_query(query, *binds)
    end

    inline(:ping)     { 0 }
    inline(:rollback) { @handle.rollback; super() }
    inline(:commit)   { @handle.commit; super()   }
  end

  class Statement < RDBI::Statement
    attr_accessor :handle

    def initialize(query, dbh)
      @handle = dbh.handle.prepare(query)
      super
    end

    def new_execution(*binds)
      rs = @handle.execute(*binds)
      ary = rs.to_a 
      # FIXME schema, columns method.
      return ary, RDBI::Schema.new
    end

    def finish
      @handle.close
      super
    end
  end
end
