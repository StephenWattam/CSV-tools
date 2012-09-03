#! /usr/bin/env ruby
#
# Large CSV merging app.  uses an index construction method to merge CSVs in excess of memory size
require 'csv'
require './lib/progress_bar.rb'
CSV_OUT = "out.csv"


LOW_MEMORY = false    # Keep memory usage to an absolute minimum (not recommended unless you have almost no RAM)
STRICT_GC = true      # Garbage collect after large operations (recommended) 


def count_items(csv)
  count = 0
  pbar = CLISpinBar.new(true)
  pbar.set_status("Counting (#{csv})...")
  pbar.render_thread(0.1)

  # Enable garbage collector stress mode, to keep memory clean
  GC.stress = true if LOW_MEMORY 

  count = CSV.read(csv, {:headers => true}).length
  # CSV.foreach(csv, {:headers => true}) do |csvin|
  #   count += 1
  # end

  # And turn it off again
  GC.stress = false  if LOW_MEMORY 
  GC.start           if STRICT_GC



  pbar.stop_thread
  print "\n" 
  return count
end

def get_field_list(csv_filename)
  csv = CSV.open(csv_filename, 'r', {:headers => true} )#CSV_OPTIONS)
  # Shift once to get past the header, if it exists
  csv.shift()
  row = csv.shift()

  list = row.headers
  csv.close

  list.each_index{ |i|
    list[i] = list[i].to_s
  }
  return list
end


def get_prefix_length(csv, key, csv_size)
  puts "Determining index block size for #{csv}:#{key}..."
  records = 0
  prefix = []
  pbar = CLIProgressBar.new(csv_size, true, true)
  pbar.render_thread(0.1)

  # Enable garbage collector stress mode, to keep memory clean
  GC.stress = true if LOW_MEMORY 

  CSV.foreach(csv, {:headers => true}) do |csvin|
    val = csvin.field(key).to_s

    # For each position, for each char, keep a count.
    pos = 0
    val.each_char{|c|
      prefix[pos]     = {} if not prefix[pos]
      prefix[pos][c]  = 0  if not prefix[pos][c]
      prefix[pos][c] += 1
      pos            += 1
    }

    # count records
    records += 1
    pbar.update_abs(records)
  end


  # And turn it off again
  GC.stress = false  if LOW_MEMORY 
  GC.start           if STRICT_GC


  pbar.stop_thread
  print "\n"
  rcount_size = [records].pack("l").size

  # From the hash of prefixes, determine the shortest possible prefix
  key_size = 0
  prefix.each{|chars|
    key_size += 1
    max = 1
    chars.each{|k, v| max = v if v > max }
    break if max == 1 # Each char is at this point used only once
  }
  throw "Cannot merge a CSV with no key values (there is nothing to merge on)" if key_size == 0

  return key_size, rcount_size
end


def build_index(cache, csv, key, key_size, csv_size)
  puts "Building index"

  # Progress bar...
  pbar = CLIProgressBar.new(csv_size, true, true)
  pbar.render_thread(0.1)
  count = 0

  # Get size in bytes, so we know when we've hit the end.
  file_size = File.size(csv)
  CSV.open(csv, {:headers => true}) do |csvin|

    # Get byte offset
    line_start = csvin.tell

    # Then read line
    while((line_start = csvin.tell) < file_size) do

      # Load the line
      val = csvin.shift()

      # Load the key up to the key size only
      val = val.field(key).to_s[0..key_size]
      
      # Save the file offset
      cache[val] = line_start
      
      #puts "#{val} => #{csvin.tell}"
      count += 1
      pbar.update_abs(count)
    end
  end
  pbar.stop_thread
  print "\n"
end

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

  # TODO: use multiple adds per transaction
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

# TODO
module MergePolicy
  class LeftMerge
  end
  
  class RightMerge
  end
  
  class InnerMerge
  end
  
  class OuterMerge
  end
  
  class CustomMerge
  end
end



#(merging A with B)
#1) min_prefix from file A   = {}
#2) write to file/hash C     = { "prefix:line" padded to n bytes } (or store this in RAM)
#3) open A, B, C             = { Read B, binary search prefix in C, fseek/read data from line 'line' in A }
#4) write shit into D (merged file)


lhs_csv = ARGV[0]
rhs_csv = ARGV[1]
csvout  = "out.csv"

# TODO: support compound keys
key1 = ARGV[2]
key2 = ARGV[3]

# The merging algorithm to use.
# TODO: use MergePolicy::whatever.
# Merging left with add columns from rhs_csv to lhs_csv, where keys match
# Merging right will add columns from lhs_csv to rhs_csv, where keys match
# Merging inner will add columns from lhs_csv and rhs_csv, merge where keys match and output BOTH, with empty values
policy = ARGV[4] ? ARGV[4] : "lmerge"

puts "Merging #{lhs_csv} into #{rhs_csv} with policy #{policy}"


puts "Finding fields in each"
lhs_fields      = get_field_list(lhs_csv)
rhs_fields      = get_field_list(rhs_csv)
output_fields   = (lhs_fields + rhs_fields).uniq
puts "LHS: #{lhs_fields.join(', ')}"
puts "RHS: #{rhs_fields.join(',')}"
puts "Output: #{output_fields.join(', ')}"

puts "Counting records in each (and validating CSV)"
lhs_records = count_items(lhs_csv)
rhs_records = count_items(rhs_csv)
puts "#{lhs_csv} (LHS): #{lhs_records} \n#{rhs_csv} (RHS): #{rhs_records}"



# build minimal index for the key we're merging with
# TODO: change rhs_csv/key2 with source/dest syntax so the various merge styles work
puts "Building index for LHS (#{lhs_csv}:#{key1})..."
key_size, rcount_size   = get_prefix_length(lhs_csv, key1, lhs_records)
block_size              = key_size + rcount_size
puts "Block size is #{block_size}, with #{lhs_records} records meaning #{(((block_size + rcount_size)*lhs_records)/1024/1024).round(2)}MB needed for index (less for RAM)."
puts "Type 'm' to use memory, or 'd' to use disk"
cache = ""
if($stdin.getc == "m") then
  puts "Using memory!"
  puts "Building cache"
  cache = CacheMethods::MemoryCache.new
else
  puts "Using disk!"
  cache = CacheMethods::DiskCache.new
end


# Build an index for the LHS CSV file using the cache we just created
build_index(cache, lhs_csv, key1, key_size, lhs_records)


############## Perform Merge #################################

puts "Building output CSV"
pbar = CLIProgressBar.new(rhs_records, true, true)
pbar.render_thread(0.1)
count = 0

CSV.open(csvout, 'w') do |out_csv|
  out_csv << output_fields

  # The one we have the index for
  lhs_csv = File.open(lhs_csv, 'r')

    # The one we don't have the index for
    CSV.foreach(rhs_csv, {:headers => true}) do |rhs_row|
      val         = rhs_row.field(key2).to_s[0..key_size]
      #puts "Key field val: #{val}"
      rhs_vals    = rhs_row.to_hash


      seek_offset   = cache[val]
      lhs_csv.seek(seek_offset)
      # puts "Seeking to #{seek_offset}"
      lhs_vals      = CSV.parse_line(lhs_csv.readline, {:headers => lhs_fields}).to_hash
      # puts "Seeking to #{seek_offset}, line: #{lhs_vals}"

      # TODO: manage the behaviour of overwrites in the two hashes
      out_vals = lhs_vals.merge(rhs_vals)
      out_line = []
      output_fields.each{|f| 
        #puts "outputting #{f} from #{out_vals}"
        out_line << out_vals[f] }
      out_csv << out_line

      count += 1
      pbar.update_abs(count)
    end
  lhs_csv.close
end
pbar.stop_thread
print "\n"



# Remove the cache file if one was used.
puts "Cleaning up cache"
cache.cleanup
cache = nil
GC.start


# End
puts "Done."
exit
