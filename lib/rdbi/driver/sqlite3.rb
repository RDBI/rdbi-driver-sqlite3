require 'rdbi'
require 'epoxy'

gem 'sqlite3-ruby'
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
      sqlite3_connect
      @preprocess_quoter = proc do |x, named, indexed|
        quote((named[x] || indexed[x]).to_s)
      end
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
        col.name         = hash['name'].to_sym
        col.type         = hash['type'].to_sym
        col.ruby_type    = hash['type'].to_sym
        col.nullable     = !(hash['notnull'] == "0")
        col.primary_key  = !(hash['pk'] == "0")
        sch.columns << col
      end

      return sch
    end

    def ping
      0
    end

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

    def quote(item)
      case item
      when NilClass
        'NULL'
      when true
        "'t'"
      when false
        "'f'"
      when Numeric
        item
      else
        "'#{::SQLite3::Database.quote(item)}'"
      end
    end

    protected

    def sqlite3_connect
      @handle = ::SQLite3::Database.new(database_name)
      @handle.type_translation = false # XXX RDBI should handle this.
    end
  end
 
  #
  # Because SQLite3's ruby implementation does not support everything that our
  # cursor implementation does, some methods, when called, will fetch the
  # entire result set. In the instance this is done, the resulting array is
  # used for all future operations.
  #
  class Cursor < RDBI::Cursor
    def initialize(handle)
      super(handle)
      @index = 0
    end

    def fetch(count=1)
      return [] if last_row?
      a = []
      count.times { a.push(next_row) }
      return a
    end

    def next_row
      val = if @array_handle
              @array_handle[@index]
            else
              @handle.next
            end

      @index += 1
      val
    end

    def result_count
      return 0 unless rewindable_result
      coerce_to_array
      @array_handle.size
    end

    def affected_count
      # sqlite3-ruby does not support affected counts
      0
    end

    def first
      if @array_handle
        @array_handle.first
      else
        @handle.first
      end
    end

    def last
      coerce_to_array
      @array_handle[-1]
    end

    def rest
      if rewindable_result
        rewind
        coerce_to_array
        oindex, @index = @index, @array_handle.size
        @array_handle[oindex, @index]
      else
        @handle.to_a
      end
    end

    def all
      if rewindable_result
        rewind
        coerce_to_array
        @array_handle
      else
        @handle.to_a
      end
    end

    def [](index)
      coerce_to_array
      @array_handle[index]
    end
    
    def last_row?
      if @array_handle
        @index == @array_handle.size
      else
        @handle.eof?
      end
    end

    def rewind
      # FIXME better exception
      raise StandardError, "rewindable_result is not true" unless rewindable_result
      @index = 0
      @handle.reset unless @handle.closed?
    end

    def empty?
      coerce_to_array
      @array_handle.empty?
    end

    def finish
      @handle.close unless @handle.closed?
    end
    
    def coerce_to_array
      unless @coerced 
        rewind
        @array_handle = @handle.to_a
        @coerced = true
      end
    end
  end

  class Statement < RDBI::Statement
    attr_accessor :handle

    class << self
      def input_type_map  
        @input_type_map  ||= RDBI::Type.create_type_hash(RDBI::Type::In)
      end

      def output_type_map
        @output_type_map ||= RDBI::Type.create_type_hash(RDBI::Type::Out)
      end
    end

    def initialize(query, dbh)
      super

      ep = Epoxy.new(query)
      @index_map = ep.indexed_binds 

      # sanitizes the query of named binds so we can use SQLite3's native
      # binder with our extended syntax. @index_map makes a reappearance in
      # new_execution().
      query = ep.quote(@index_map.compact.inject({}) { |x,y| x.merge({ y => nil }) }) { '?' }

      @handle = check_exception { dbh.handle.prepare(query) }
      @input_type_map  = self.class.input_type_map 
      @output_type_map = self.class.output_type_map

      prep_finalizer { @handle.close unless @handle.closed? }
    end

    def new_modification(*binds)
      binds = RDBI::Util.index_binds(binds, @index_map) 

      rs = check_exception { @handle.execute(*binds) }

      return 0
    end

    def new_execution(*binds)
      binds = RDBI::Util.index_binds(binds, @index_map)

      rs = check_exception { @handle.execute(*binds) }

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

      return Cursor.new(rs), this_schema, @output_type_map
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
