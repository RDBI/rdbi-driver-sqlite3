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
      sqlite3_connect
    end

    def reconnect
      super
      sqlite3_connect
    end

    def disconnect
      super
      @handle.close
    end

    def transaction(&block)
      raise RDBI::TransactionError, "already in a transaction" if in_transaction?
      @handle.transaction
      super
    end

    def new_statement(query)
      sth = Statement.new(query, self)
      return sth
    end

    def preprocess_query(query, *binds)
      mutex.synchronize { @last_query = query }

      hashes, binds = binds.partition { |x| x.kind_of?(Hash) }
      total_hash = hashes.inject({}) { |x, y| x.merge(y) }

      ep = Epoxy.new(query)
      ep.quote(total_hash) { |x| ::SQLite3::Database.quote((total_hash[x] || binds[x]).to_s) }
    end

    def schema
      sch = { }
      execute("SELECT name, type FROM sqlite_master WHERE type='table' or type='view'").fetch(:all).each do |row|
        table_name_sym, table_name, table_type_sym = row[0].to_sym, row[0], row[1].to_sym
        sch[table_name_sym] = table_schema(table_name, table_type_sym)
      end
      return sch
    end

    def table_schema(table_name, type = nil) # overloaded for performance
      sch = RDBI::Schema.new([], [], type)
      sch.tables << table_name.to_sym

      unless sch.type
        sch.type = execute("select type from sqlite_master where (type='table' or type='view') and name=?", table_name.to_s).fetch(:first)[0].to_sym rescue nil
        return nil unless sch.type
      end

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

    inline(:ping) { 0 }

    def rollback
      raise RDBI::TransactionError, "not in a transaction during rollback" unless in_transaction?
      @handle.rollback
      super()
    end

    def commit
      raise RDBI::TransactionError, "not in a transaction during commit" unless in_transaction?
      @handle.commit
      super()
    end

    protected

    def sqlite3_connect
      @handle = ::SQLite3::Database.new(database_name)
      @handle.type_translation = false # XXX RDBI should handle this.
    end
  end

  class Statement < RDBI::Statement
    extend MethLab

    attr_accessor :handle

    def initialize(query, dbh)
      super

      ep = Epoxy.new(query)
      @index_map = ep.indexed_binds 

      # sanitizes the query of named binds so we can use SQLite3's native
      # binder with our extended syntax. @index_map makes a reappearance in
      # new_execution().
      query = ep.quote(@index_map.compact.inject({}) { |x,y| x.merge({ y => nil }) }) { '?' }

      @handle = check_exception { dbh.handle.prepare(query) }
      @input_type_map  = RDBI::Type.create_type_hash(RDBI::Type::In)
      @output_type_map = RDBI::Type.create_type_hash(RDBI::Type::Out)
    end

    def new_execution(*binds)

      # XXX is there a patron saint of being too clever? I don't like this
      # code.
      
      # FIXME move to RDBI::Util or something.
      hashes, binds = binds.partition { |x| x.kind_of?(Hash) }
      hash = hashes.inject({}) { |x, y| x.merge(y) }
      hash.keys.each do |key| 
        if index = @index_map.index(key)
          binds.insert(index, hash[key])
        end
      end

      # but this code is still ok.

      rs = check_exception { @handle.execute(*binds) }
      ary = rs.to_a

      # FIXME type management
      columns = rs.columns.zip(rs.types)
      columns.collect! do |col|
        newcol = RDBI::Column.new
        newcol.name = col[0].to_sym
        newcol.type = col[1]
        newcol.ruby_type = (col[1].to_sym rescue nil)
        newcol
      end

      this_schema = RDBI::Schema.new
      this_schema.columns = columns

      return ary, this_schema, @output_type_map
    end

    def finish
      @handle.close rescue nil
      super
    end

    protected

    def check_exception(&block)
      begin
        yield
      rescue ArgumentError => e
        if dbh.handle.closed?
          raise RDBI::DisconnectedError, "database is disconnected"
        else
          raise e
        end
      end
    end
  end
end
