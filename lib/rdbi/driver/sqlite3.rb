require 'rdbi'
require 'epoxy'
require 'methlab'
require 'sqlite3'

class RDBI::Driver::SQLite3 < RDBI::Driver
  def initialize(*args)
    super(Database, *args)
  end
end

class RDBI::Driver::SQLite3 < RDBI::Driver
  class Database < RDBI::Database
    extend MethLab

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

    def transaction(&block)
      @handle.transaction
      super
    end

    def new_statement(query)
      Statement.new(query, self)
    end

    def preprocess_query(query, *binds)
      mutex.synchronize { @last_query = query } 

      ep = Epoxy.new(query)
      ep.quote { |x| ::SQLite3::Database.quote(binds[x].to_s) }
    end

    inline(:ping)     { 0 }
    inline(:rollback) { @handle.rollback; super() }
    inline(:commit)   { @handle.commit; super()   }
  end

  class Statement < RDBI::Statement
    extend MethLab

    attr_accessor :handle

    def initialize(query, dbh)
      @handle = dbh.handle.prepare(query)
      super
    end

    def new_execution(*binds)
      rs = @handle.execute(*binds)
      ary = rs.to_a 

      # FIXME type management
      columns = rs.columns.zip(rs.types)
      columns.collect! do |col|
        newcol = RDBI::Column.new
        newcol.name = col[0]
        newcol.type = col[1]
        newcol
      end

      this_schema = RDBI::Schema.new
      this_schema.columns = columns

      return ary, this_schema
    end

    def finish
      @handle.close
      super
    end
  end
end