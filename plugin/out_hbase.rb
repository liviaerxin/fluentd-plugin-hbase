module Fluent
  class HBaseOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('hbase', self)
    def initialize
      super
      require 'massive_record'
      require 'massive_record/adapters/thrift/hbase/hbase_types'
    end
    # Format dates with ISO 8606 by default 
    # http://www.w3.org/TR/NOTE-datetime
    config_param :time_format, :string, :default => '%Y-%m-%dT%H:%M:%S.%L%:z'
    config_param :hbase_host, :string, :default => 'localhost'
    config_param :hbase_port, :integer, :default => 9090
    config_param :hbase_table, :string, :default=> '%{tag}'
    config_param :hbase_column_family, :string, :default => 'default'
    config_param :hbase_key, :string, :default => '%{uuid}'
    config_param :hbase_ignore_columns, :string, :default => nil
    attr_accessor :column_family
    attr_accessor :tables
    ##################################################
    #overrided functions from Fluent::BufferedOutput
  
    def configure(conf)
      super
      @tables = Hash.new
    end
    def start
      super
      @conn = MassiveRecord::Wrapper::Connection.new(:host => @hbase_host, :port => @hbase_port)
    end
    def format(tag, time, record)
      #Convert all values in the record to string type in order to save them into HBase. 
      record.each do |k,v| 
           record[k] = v.to_s
      end
      [tag, time, record].to_msgpack
    end
    def write(chunk)
      chunk.msgpack_each {|tag, time, record|
	r_time = 9999999999 - time
        binding = {tag:tag, time:time, reverse_time:r_time, uuid:SecureRandom.hex(8)}
        inner_merge(binding, record, 'record')
        table_name = @hbase_table%binding
        column_family = @hbase_column_family%binding
	begin
    # heavy processing
	  id = @hbase_key%binding
        rescue Exception => e
          #if @hbase_key is not fully qualified discard these record
	  @log.error <<MESSAGE
caught exception #{e}!
MESSAGE
	  next
        end
	#id = @hbase_key%binding
	
	unless hbase_ignore_columns.nil?
          hbase_ignore_columns.split(',').map(&:strip).each{|col_to_ignore|
            record.delete(col_to_ignore)
          }
        end
        row = create_hbase_row(record, column_family)
        row.id = id
        row.table = get_hbase_table(tag, table_name, column_family)
        begin
          row.save

	#catch connection error
	rescue MassiveRecord::Wrapper::Errors::ConnectionException => e
	  @log.error <<MESSAGE
Connection error: "#{e}"
MESSAGE
	#throw an exception to let td-agent apply the retry method
	  raise MassiveRecord::Wrapper::Errors::ConnectionException
	#catch the exception table not existing  
	rescue Apache::Hadoop::Hbase::Thrift::IOError => e 
	  $log.error <<MESSAGE 
Table "#{table_name}" is not exists now while it was exists when system startup. An new table "#{table_name}" is going to be created now
MESSAGE
	  ############################ Test whether the connection status
	  
	  
	    
	  @tables.delete(tag)
          row.table = get_hbase_table(tag, table_name, column_family)
          row.save
        end
      }
    end
    ##################################################
    #helper functions
    def inner_merge(target, src, name)
      src.each{|key, val|
        target[ :"#{name}['#{key}']" ] = val
      }
    end
    
    def create_hbase_row(record, column_family)
        event = {}
        record.each {|column, value|
          (event[column_family.intern] ||= {}).update({column => value})
        }
        row = MassiveRecord::Wrapper::Row.new
        row.values = event
        return row
    end
    
    def get_hbase_table(tag, table_name, column_family)
      if @tables[tag].nil?
        @tables[tag] = create_hbase_table(table_name, column_family)
      end
      return @tables[tag]
    end
    def create_hbase_table(table_name, column_family)
      table = MassiveRecord::Wrapper::Table.new(@conn, 
table_name.intern)
      unless table.exists?
        column = MassiveRecord::Wrapper::ColumnFamily.new(column_family)
        table.column_families.push(column)
        table.save
      end
      return table
    end
  end
end
