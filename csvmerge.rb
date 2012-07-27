#/usr/bin/env ruby
#
# Large CSV merging app.  uses an index construction method to merge CSVs in excess of memory size
require 'csv'
require './lib/progress_bar.rb'
CSV_OUT = "out.csv"

def count_items(csv)
  count = 0
  pbar = CLISpinBar.new(true)
  pbar.set_status("Counting (#{csv})...")
  pbar.render_thread(0.1)
  CSV.foreach(csv, {:headers => true}) do |csvin|
    count += 1
  end
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
  pbar.render_thread(0.01)
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


class Cache
  def initialize(csv, key)
    @csv = csv
    @key = key
  end

  def get_line(key)
  end
end

class MemoryCache < Cache
  def initialize(csv, key, key_size, csv_size)
    super(csv, key)
    @cache = {}
    pbar = CLIProgressBar.new(csv_size, true, true)
    pbar.render_thread(0.01)
    count = 0
    CSV.open(csv, {:headers => true}) do |csvin|
      while(val = csvin.shift()) do 
        val = val.field(key).to_s[0..key_size]
        @cache[val] = csvin.tell # get file offset
        #puts "#{val} => #{csvin.tell}"
        count += 1
        pbar.update_abs(count)
      end
    end
    pbar.stop_thread
    print "\n"
  end

  def get_line(key)
    @cache[key]
  end
end

class DiskCache < Cache
  def initialize(csv, key, key_size, filename="/tmp/csvmerge.tmp")
    super(csv, key)
    ifile = File.new(filename, "wb", 0644)
    ifile.close
  end

  def get_line(key)
    puts "DISK CACHE STUB"
  end

end

#(merging A with B)
#1) min_prefix from file A   = {}
#2) write to file/hash C     = { "prefix:line" padded to n bytes } (or store this in RAM)
#3) open A, B, C             = { Read B, binary search prefix in C, fseek/read data from line 'line' in A }
#4) write shit into D (merged file)


csv1 = ARGV[0]
csv2 = ARGV[1]
csvout = "out.csv"


key1 = ARGV[2]
key2 = ARGV[3]

# The merging algorithm to use.
# Merging left with add columns from csv2 to csv1, where keys match
# Merging right will add columns from csv1 to csv2, where keys match
# Merging inner will add columns from csv1 and csv2, merge where keys match and output BOTH, with empty values
policy = ARGV[4] ? ARGV[4] : "lmerge"

puts "Merging #{csv1} into #{csv2} with policy #{policy}"


puts "Finding fields in each"
lhs_fields = get_field_list(csv1)
rhs_fields = get_field_list(csv2)
puts "LHS: #{lhs_fields}"
puts "RHS: #{rhs_fields}"

puts "Counting records in each (and validating CSV)"
lhs_records = count_items(csv1)
rhs_records = count_items(csv2)
puts "#{csv1} (LHS): #{lhs_records} \n#{csv2} (RHS): #{rhs_records}"



# build minimal index for the key we're merging with
# TODO: change csv2/key2 with source/dest syntax so the various merge styles work
puts "Building index for LHS (#{csv1}:#{key1})..."
key_size, rcount_size = get_prefix_length(csv1, key1, lhs_records)
block_size = key_size + rcount_size
puts "Block size is #{block_size}, with #{lhs_records} records meaning #{(((block_size + rcount_size)*lhs_records)/1024/1024).round(2)}MB needed for index (less for RAM)."
puts "Type 'm' to use memory, or 'd' to use disk"
cache = ""
if($stdin.getc == "m") then
  puts "Using memory!"
  puts "Building cache"
  cache = MemoryCache.new(csv1, key1, key_size, lhs_records)
else
  puts "Using disk!"
  cache = DiskCache.new(csv1, key1, key_size)
end

############## Perform Merge #################################

output_fields = (lhs_fields + rhs_fields).uniq
puts "Output fields: #{output_fields}"


pbar = CLIProgressBar.new(rhs_records, true, true)
pbar.render_thread(0.01)
count = 0

CSV.open(csvout, 'w') do |out_csv|
  out_csv << output_fields

  # The one we have the index for
  lhs_csv = File.open(csv1, 'r')

    # The one we don't have the index for
    CSV.foreach(csv2, {:headers => true}) do |rhs_row|
      val         = rhs_row.field(key2).to_s[0..key_size]
      #puts "Key field val: #{val}"
      rhs_vals  = rhs_row.to_hash


      seek_offset   = cache.get_line(val)
      lhs_csv.seek(seek_offset)
      lhs_vals      = CSV.parse_line(lhs_csv.readline, {:headers => lhs_fields}).to_hash
      #puts "Seeking to #{seek_offset}, line: #{lhs_vals}"

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
