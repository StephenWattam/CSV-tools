#! /usr/bin/env ruby

require 'csv'
# Loads a CSV and splits it along columns

OUTPUT_DIVISOR = 512


def parse_commandline_opts
  filename = ARGV[0]
  output_files = {}

  position = 0
  current_filename = nil
  ARGV[1..-1].each{|arg|
    # The first item is the filename
    if(position+=1) == 1 then
      current_filename                  = arg
      output_files[current_filename]    = {}  
    end

    # All other items are fields.
    if position > 1 and arg != "--" 
      (output_files[current_filename][:fields] ||= []) << arg  
    end

    # break on separator
    position = 0            if arg == "--"                    
  }

  return filename, output_files
end

filename, output_files = parse_commandline_opts

puts "Splitting file #{filename}"
puts "into: "
# Print some info
output_files.each{|file, opts|
  puts "    #{file}: #{opts[:fields].join(', ')}"
}
puts ""


# Open output files
puts "Opening output files..."
output_files.each{|file, opts|

  # Open CSV for writing
  opts[:handle] = CSV.open(file, 'w')

  # Push headers
  opts[:handle] << opts[:fields]
}
puts "Done."



puts "Parsing output..."
count = 0
CSV.foreach(filename, {:headers => true}) do |cin|

  # Loop through outputs and output this line.
  output_files.each{|file, opts|
    line = []
    opts[:fields].each{|f|
      line << cin.field(f)
    }
    opts[:handle] << line
  }

  print "\rLine #{count} " if (count+=1) % OUTPUT_DIVISOR == 0
end
print "\n"
puts "Done."




puts "Closing output files..."
output_files.each{|file, opts|
  opts[:handle].close
}
puts "Done."
