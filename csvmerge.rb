#! /usr/bin/env ruby
#   TODO
#
# 1. Structure using classes
# 2. Better command line option handling
# 3. Merge policies that are able to better select data
# 4. More efficient disk storage systems (+my/sql/ite)
# 5. Threading on disk reads
# 6. ETAs and better output
# 7. Support for compound keys
#



#
# Large CSV merging app.  uses an index construction method to merge CSVs in excess of memory size
require 'csv'
require './lib/progress_bar.rb'
CSV_OUT = "out.csv"


LOW_MEMORY = false    # Keep memory usage to an absolute minimum (not recommended unless you have almost no RAM)
STRICT_GC = true      # Garbage collect after large operations (recommended) 






module CacheMethods
  class Cache

    # Add an item to the cache
    #
    # Key will be the CSV minimal key
    # offset will be the file offset, int.
    def []=(key, offset)
      puts "STUB: []= in CacheMethods::Cache"
    end

    # Retrieve a file offset using a key
    #
    # key will be the CSV minimal key
    def [](key)
      puts "STUB: [] in CacheMethods::Cache"
    end

    # Signal that we no longer wish to make any 
    # changes
    #
    # This allows the cache to optimise itself, or
    # switch to read only mode.
    def finalise
    end

    # Signal that we no longer wish to use the store
    # This allows things to remove their disk stuff
    # and/or clean up RAM
    def cleanup
    end




  end

  class MemoryCache < Cache
    def initialize
      @cache = {}
    end

    def []=(key, offset)
      @cache[key] = offset
    end

    def [](key)
      @cache[key]
    end
  end

  class DiskCache < Cache
    def initialize(filename="/tmp/csvmerge.tmp", transaction_size=10000)
      require 'pstore'
      @cache            = PStore.new(filename, false)
      @read_only        = false
      @pending          = {}
      @transaction_size = transaction_size.to_i
    end

    def []=(key, offset)
      raise "Cannot store!" if @read_only

      # Write into the memory cache-cache
      @pending[key] = offset

      # If we hit the transaction count, write
      write_to_disk if @pending.size >= @transaction_size
    end

    def [](key)
      @cache.transaction(true){|cache|
        return cache[key]
      }
    end

    def finalise
      write_to_disk
      @read_only = true
    end

    def cleanup
      File.rm(@cache.path)
    end

  private
    def write_to_disk
      @cache.transaction(false){|cache|
        @pending.each{|k, v|
          cache[k] = v
        }
      }
    end

  end
end

# Provides a wrapper around CSV
# that will perform various checks and descriptive statistical ops.
class KeyValueCSV 

  attr_reader :keys, :filename

  def initialize(filename, keys)
    @filename = filename
  
    # Load keys
    @keys = keys
    # Check all the keys are there
    @keys.each{|k|
      raise "Key is not in fields list" if not fields.include?(k)
    }
  end


  def minimal_keys
    compute_minimal_keys if not @minimal_keys
    @minimal_keys
  end

  # Total size of the key
  def key_size
    compute_minimal_keys if not @key_length
    @key_length 
  end

  # Retrieve a key from a line
  def get_minimal_key(line)
    compute_minimal_keys if not @minimal_keys
   
    str = ""
    minimal_keys.each{|k, size|
      str += line.field(k).to_s[0..size]
    }
    return str
  end

  def fields
    build_fields if not @fields
    @fields
  end

  def count
    build_count if not @count
    @count
  end

  def to_s
    return "#{@filename}"
  end

  def cache=(cache)
    raise "Cannot set cache when seeking" if @handle
    @cache = cache
    build_index
  end
 
  def seek_retrieval(&block)
    File.open(@filename, 'r') do |handle|
      @handle = handle
      yield
      @handle = nil
    end
  end

  def []=(k, v)
    raise "No Cache!" if not @cache
    @cache[k] = v
  end

  def [](k)
    raise "Cannot retrieve from cache if not seeking" if not @handle
    raise "No Cache" if not @cache

    # Seek to the location in the cache and return a line
    if seek_to = @cache[k] then
      @handle.seek(seek_to)
      return CSV.parse_line(@handle.readline, {:headers => fields}).to_hash
    else
      return nil
    end
  end

private
    
  
  # Construct an index.  Should not need to be
  # overidden
  def build_index
    puts "Building index for #{@filename}"

    # Progress bar...
    pbar = CLIProgressBar.new(count, true, true)
    pbar.render_thread(0.1)
    count = 0

    # Get size in bytes, so we know when we've hit the end.
    file_size = File.size(@filename)
    CSV.open(@filename, {:headers => true}) do |csvin|

      # Get byte offset
      line_start = csvin.tell

      # Then read line
      while((line_start = csvin.tell) < file_size) do

        # Load the line
        line = csvin.shift()

        # Load the key up to the key size only
        key = get_minimal_key(line)
        
        # Save the file offset
        # TODO: ensure random access of the cache is possible
        $stderr.puts "WARNING: Key on line #{count} of #{@filename} collides with key at byte #{@cache[key]}." if @cache[key]
        @cache[key] = line_start
        
        pbar.update_abs(count+=1)
      end
    end
    pbar.stop_thread
    print "\n"
  end
  
  
  def compute_minimal_keys
    @minimal_keys = {}


    puts "Determining index block size for #{@filename}..."

    puts "Building prefix tables..."
    # Set up per-key prefix table and max length measure
    prefix_tables = {}
    max_lengths   = {}
    @keys.each{ |k| 
      max_lengths[k]    = 0   # length of the field
      prefix_tables[k]  = []  # position-usage measure
    }

    # Progress bar
    pbar      = CLIProgressBar.new(count, true, true)
    pbar.render_thread(0.1)

    # Enable garbage collector stress mode, to keep memory clean
    GC.stress = true if LOW_MEMORY 

    count = 0
    CSV.foreach(@filename, {:headers => true}) do |csvin|
      prefix_tables.each{ |key, prefix|
        val = csvin.field(key).to_s

        # For each position, for each char, keep a count.
        pos = 0
        val.each_char{ |c|
          prefix[pos]     = {} if not prefix[pos]
          prefix[pos][c]  = 0  if not prefix[pos][c]
          prefix[pos][c] += 1
          pos            += 1
        }

        # Check on the maximum length for this field
        max_lengths[key] = pos if max_lengths[key] < pos
      }

      # count records
      pbar.update_abs(count += 1)
    end


    # And turn it off again
    GC.stress = false  if LOW_MEMORY 
    GC.start           if STRICT_GC

    # Stop the progress bar
    pbar.stop_thread
    print "\n"

    puts "Computing minimum size for #{self}."
    # For each key, compute the minimal size from the prefix table
    prefix_tables.each{|key, prefix|

      # From the hash of prefixes, determine the shortest possible prefix
      key_size = 0
      prefix.each{|chars|
        key_size  += 1
        max        = 1
        # puts "prefix for field #{key}, filename #{@filename}: position #{key_size} sees #{prefix[key_size-1].size} #{chars.size} different chars."
        chars.each{|k, v|  max = v if v > max  }
        break if max == 1 # Each char is at this point used only once
      }
      throw "No key values for field #{key} in #{@filename}" if key_size == 0

      # If the final character position has only seen one character
      # then this field is NOT unique
      # non_unique_fields << key if prefix[key_size-1].length == 1
      if prefix[key_size-1].length == 1 then
        puts "WARNING: field '#{key}' in #{@filename} is uninformative, with an entropy of 0."
      end

      # Write the minimal key size for this key
      @minimal_keys[key] = key_size 
    }

    # Lastly, compute total key length
    @key_length = minimal_keys.values.inject(0, :+)
  end

  def build_count
    count = 0
    pbar = CLISpinBar.new(true)
    pbar.set_status("Counting (#{@filename})...")
    pbar.render_thread(0.1)

    # Enable garbage collector stress mode, to keep memory clean
    GC.stress = true if LOW_MEMORY 

    count = CSV.read(@filename, {:headers => true}).length
    # CSV.foreach(@filename, {:headers => true}) do |csvin|
    #   count += 1
    # end

    # And turn it off again
    GC.stress = false  if LOW_MEMORY 
    GC.start           if STRICT_GC

    pbar.stop_thread
    print "\n"

    @count = count
  end

  def build_fields
    csv = CSV.open(@filename, 'r', {:headers => true} )
    # Shift once to get past the header, if it exists
    csv.shift()
    row = csv.shift()

    # Then list headers and close
    list = row.headers
    csv.close

    # Ensure they're strings
    @fields = list.map!{|x| x.to_s }
  end

end


# TODO
# The merging algorithm to use.
# TODO: use MergePolicy::whatever.
# Merging left with add columns from rhs_csv to lhs_csv, where keys match
# Merging right will add columns from lhs_csv to rhs_csv, where keys match
# Merging inner will add columns from lhs_csv and rhs_csv, merge where keys match and output BOTH, with empty values


module MergePolicy
  class Merge
    def initialize(lhs, rhs)
      @lhs, @rhs  = lhs, rhs
    end

    # Output fields as they will be, i.e. for the header.
    def fields
      @lhs.fields + (@rhs.fields - @rhs.keys).map{|x| "rhs.#{x}"}
    end

    def to_s
      "Merge"
    end

    def fields
      (@rhs.fields + @lhs.fields).uniq
    end

    # Merge a single line
    def merge_line(lhs_line, rhs_line)
      line = []

      # Add all left hand side items
      @rhs.fields.each{ |f| line << rhs_line[f] }
      # and only non-key fields from the RHS
      (@lhs.fields - @lhs.keys).each{|f| line << lhs_line[f] }

      return line
    end
  end

  
  # TODO FIXME!
  # This should check that the LHS has NO DUPLICATE entries in the keys
  # If it does, only the first will be taken (i.e. no error).
  # XXX XXX XXX
  class LeftMerge < Merge
    def to_s
      "Left Merge"
    end

    def accept_line?(lhs_line, rhs_line)
      # Left outer joins returns ALL from LHS, even if RHS is empty
      true
    end
  end
  
  class InnerMerge < Merge
    def to_s
      "Inner Merge"
    end

    def accept_line?(lhs_line, rhs_line)
      # Inner join returns rows ONLY if both have data
      (not lhs_line.empty?) and (not rhs_line.empty?)
    end
  end
 
  # TODO
  #
  # An outer join will output even if
  # one half of the key is missing,
  # but match up what it can
#  class OuterMerge < Merge
#    def to_s
#      "Outer Merge"
#    end
#
#    # Accept ALL, in fact, generate some.
#    # currently impossible using this algorithm
#    def accept_line?(lhs_line, rhs_line)
#    end
#  end
 
  # TODO:
  # 
  # A cross join is the cartesian product of
  # both files.
  #
  # Currently not possible with the framework as it is
#  class CrossMerge < Merge
#    def to_s
#      "Custom Merge"
#    end
#    def accept_line?(lhs_line, rhs_line)
#    end
#  end
end




# TODO: support compound keys from the command line
lhs     = KeyValueCSV.new(ARGV[0], [ARGV[2]])
rhs     = KeyValueCSV.new(ARGV[1], [ARGV[3]])
merge   = eval("MergePolicy::#{ARGV[4] ? ARGV[4] : 'LeftMerge'}.new(lhs, rhs)")


puts "\nMerging #{lhs} into #{rhs} with policy #{merge}"



puts "\nFinding fields in each"
puts "   LHS: #{lhs.fields.join(', ')}"
puts "   RHS: #{rhs.fields.join(', ')}"
puts "Output: #{merge.fields.join(', ')}"



puts "\nCounting records in each (and validating CSV)"
puts "#{lhs} (LHS): #{lhs.count}"
puts "#{rhs} (RHS): #{rhs.count}"


puts "Finding Minimal Key Lengths for #{rhs}..."
rhs.minimal_keys.each{|k, v|
  puts " KEY #{rhs}: #{k} => #{v}"
}


puts "Key size is #{rhs.key_size}B, with #{rhs.count} records this means #{(((rhs.key_size + [rhs.count].pack('l').size)*rhs.count)/1024/1024).round(2)}MB needed for index."
puts "Type 'm' to use memory, or 'd' to use disk"
cache = ""

if($stdin.getc == "m") then
  puts "Using memory!"
  cache = CacheMethods::MemoryCache
else
  puts "Using disk!"
  cache = CacheMethods::DiskCache
end
rhs.cache = cache.new



############## Perform Merge #################################
CSV.open(CSV_OUT, 'w') do |out_csv|
  out_csv << merge.fields

  
  puts "Building output CSV..."
  pbar = CLIProgressBar.new(lhs.count, true, true)
  pbar.render_thread(0.1)
  count = 0


  # Open a transaction for the rhs file,
  # in readiness for reading from keys
  rhs.seek_retrieval{

    # The one we don't have the index for
    CSV.foreach(lhs.filename, {:headers => true}) do |lhs_row|

      # Generate a key from the row
      key         = rhs.get_minimal_key(lhs_row)
    
      # Look up the value from the LHS cache
      # This uses file seeking, so is quickish
      # and low on memory
      rhs_vals    = rhs[key] || {}

        
      # Ensure the RHS vals are in hash form 
      lhs_vals    = lhs_row.to_hash

      # Merge according to policy and output
      out_csv << merge.merge_line(lhs_vals, rhs_vals) if merge.accept_line?(lhs_vals, rhs_vals)

      pbar.update_abs(count+=1)
    end
  }


  pbar.stop_thread
  print "\n"
end


# End
puts "Done."
exit
