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

    def schema
      sch = []
      execute("SELECT name FROM sqlite_master WHERE type='table'").fetch(:all).each do |row|
        sch << table_schema(row[0])
      end
      return sch
    end

    def table_schema(table_name)
      sch = RDBI::Schema.new([], [])
      sch.tables << table_name.to_sym
      @handle.table_info(table_name) do |hash|
        col = RDBI::Column.new
        col.name       = hash['name'].to_sym
        col.type       = hash['type'].to_sym
        col.ruby_type  = hash['type'].to_sym
        col.nullable   = !(hash['notnull'] == "0")
        sch.columns << col
      end

      return sch
    end

    inline(:ping)     { 0 }
    inline(:rollback) { @handle.rollback; super() }
    inline(:commit)   { @handle.commit; super()   }
  end

  class Statement < RDBI::Statement
    extend MethLab

    attr_accessor :handle

    def initialize(query, dbh)
      super
      @handle = dbh.handle.prepare(query)
      @input_type_map  = RDBI::Type.create_type_hash(RDBI::Type::In)
      @output_type_map = RDBI::Type.create_type_hash(RDBI::Type::Out)
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
        newcol.ruby_type = col[1].to_sym
        newcol
      end

      this_schema = RDBI::Schema.new
      this_schema.columns = columns

      return ary, this_schema, @output_type_map
    end

    def finish
      @handle.close
      super
    end
  end
end
