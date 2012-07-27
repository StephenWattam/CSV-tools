#!/usr/bin/env ruby
# Merge two csv files using a common key field.
# mergecsv file1 file2 outfile keyfield [keyfield2]
# $ ./merge_csv.rb sample_orig.csv sample_addit.csv sample_output.csv key

# --------------------------------------------------------------------------
require 'csv'
require 'logger'

# --------------------------------------------------------------------------
CSV_OPTIONS = {:headers => true}
NOTIFY_AT   = 1024

# --------------------------------------------------------------------------
def quit_clean(msg)
  $stdout.puts(msg)
  exit(0)
end

def quit_fail(error)
  $stderr.puts(error)
  exit(1)
end

def get_field_list(csv_filename)
  csv = CSV.open(csv_filename, 'r', CSV_OPTIONS)
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

# info
log = Logger.new(STDOUT)
log.level = Logger::INFO
# --------------------------------------------------------------------------
# Files
original_filename   = ARGV[0].to_s #most of the data
additional_filename = ARGV[1].to_s #additional fields
output_filename     = ARGV[2].to_s #additional fields

# TODO: split by comma and allow compound lists
# get keys
key_field           = ARGV[3].to_s 

# --------------------------------------------------------------------------
# File existence input checks
if not File.exist?(original_filename) then
  quit_fail("Original File '#{original_filename}' does not exist.")
end
if not File.exist?(additional_filename) then
  quit_fail("Additional info file '#{additional_filename}' does not exist.")
end
if File.exist?(output_filename) then
  quit_fail("Output file exists, I will not overwrite it.")
end  

# load some metadata on the csv files
original_fields   = get_field_list(original_filename)
additional_fields = get_field_list(additional_filename)
resultant_fields = ((original_fields + additional_fields) - [key_field]).unshift(key_field)


# Output plans for the opportunity to turn down the output
puts "Key field        : #{key_field}"
puts "Original Fields  : #{original_fields.join(", ")}"
puts "Additional Fields: #{additional_fields.join(", ")}"
puts "Output Fields    : #{resultant_fields.join(", ")}"
if not original_fields.include?(key_field) or 
   not additional_fields.include?(key_field) then
  quit_fail("Key fields not found in both files.")
end
puts "\nHit ^C to cancel now if these are unsatisfactory."
$stdin.getc



puts "Loading auxiliary file details into memory."
# --------------------------------------------------------------------------
# load one CSV into memory (the small one)
lines = Hash.new()
c = 0
CSV.foreach(additional_filename, CSV_OPTIONS) do |row|
  keyval = row.field(key_field)
  lines[keyval] = row
  log.info "Complete: #{c}" if (c % NOTIFY_AT) == 0
  c += 1 
end


puts "Looping over original CSV."
# --------------------------------------------------------------------------
# load one CSV into memory (the small one)
c = 0
e = 0
CSV.open(output_filename, 'w') do |out|
  out << resultant_fields
  CSV.foreach(original_filename, CSV_OPTIONS) do |row|
    line = []
    error = false

    # choose where the values comes from to enter
    # into each field
    resultant_fields.each{ |field|
      if original_fields.include?(field)
        line << row.field(field)
      elsif additional_fields.include?(field)
        line << lines[row.field(key_field)].field(field)
      else
        error = true
      end
    }

    # Handle error to notify user
    e += 1 if error
    out << line if not error

    # Notify
    log.info "Complete: #{c}.  Error: #{e}." if (c % NOTIFY_AT) == 0

    c += 1
  end
end

# Quit and tidy up
quit_clean("Done.")
