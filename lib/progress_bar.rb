# renders a wget-like progress bar.
# Also comes in a threaded form for notification-based updates
# and a referential form that watches a variable
require 'thread'
class CLIBar

  attr_reader :spinner

  MIN_WIDTH = 20
  MIN_BAR_WIDTH = 5

  SPINNER    = '|/-\\'
  #SPINNER    = "bdqp"
  SPIN_BEGIN = "["
  SPIN_END   = "]"
  def initialize(max, spinner=true, width=:fit, dev=$stdout)
    @mutex = Mutex.new
    set_max(max)
    @device = dev
    
    # in characters
    @width = width
    @val = 0
    @vc_type = :abs


    @stop_threaded_updates = false
    @use_callbacks_for_updates = false
    @spincount = 0
    set_spinner(spinner)
    set_status("")

  end

  def set_status_callback(scb)
    @mutex.synchronize{
      @status_callback = scb
    }
  end

  def set_value_callback(vcb)
    @mutex.synchronize{
      @value_callback = vcb
    }
  end
  
  def set_end_callback(ecb)
    @mutex.synchronize{
      @end_callback = ecb
    }
  end

  def set_use_callbacks(cb)
    @mutex.synchronize{
      @use_callbacks_for_updates = (cb == true)
    }
  end

  def set_max(max)
    @mutex.synchronize{
      if max > 0
        @max = max
      else
        @max = 100
      end
    }
  end

  def render_thread(sleeptime)
    raise "A thread is currently running.  Please stop it first using stop_thread()" if @thread

    @thread = Thread.new {start_render_loop(sleeptime)}
    return @thread
  end

  def stop_thread
    #tell the thread to die
    @mutex.synchronize{
      @stop_threaded_updates = true
    }

    #wait 1 second for the thread to kill itself cleanly
    10.times{|n|
      if (not @thread) or (n == 9)
        break
      end
      sleep 0.1
    }

    # murder it if not.
    @thread.kill if @thread
    @thread = nil
  end
 
  def set_spinner(spin)
    @mutex.synchronize{
      @spinner = (spin == true)
    }
  end 

  def set_status(status)
    @mutex.synchronize{
      @status = status
    }
  end

  def update_abs_and_render(status, value)
    update_abs(value)
    set_status(status)
    render
  end

  def update_rel_and_render(status, value)
    update_delta(value)
    set_status(status)
    render
  end

  # update to an absolute value
  def update_abs(val)
    @mutex.synchronize{
      if val > @max or val < 0
        @val = @max 
      else
        @val = val
      end
    }
  end

  # or a change therein.
  def update_delta(dval)
    @mutex.synchronize{
      update_abs(@val + dval) 
    }
  end

  def update_callbacks
    @mutex.synchronize{
      update_via_callbacks
    }
  end
  
  # return the string but do not render it
  def get_line
    return calculate_barstring
  end
  
  # render the string to the terminal
  def render
    output(calculate_barstring)
  end

  private

  def start_render_loop(sleeptime)
    estring = ""
    while(@stop_threaded_updates == false)
      update_via_callbacks if @use_callbacks_for_updates
      render
      sleep(sleeptime)
    
      
      @stop_threaded_updates = @end_callback.call if @use_callbacks_for_updates and @end_callback
    end

    # stop the threadiness
    @stop_threaded_updates = false
    @thread                = false
  end

  # update internal values using callbacks.
  def update_via_callbacks
    if @value_callback
      if @vc_type == :abs
        update_abs(@value_callback.call) 
      else
        update_delta(@value_callback.call) 
      end
    end

    if @status_callback
      set_status(@status_callback.call)
    end
  end

  # x as a proportion of max.
  # maps to a character width 
  def map_to_char(x, xmax, barlength)
    charpos = (x.to_f/xmax.to_f) * barlength.to_f
    return charpos.round
  end

  # output to a terminal on one line.
  def output(str) 
    if @device.tty?
      @device.print("\r#{str}")
    else
      @device.puts(str)
    end
  end

  # get the spinner state
  # updates on every call.
  def get_spinner_string
    @spincount = (@spincount + 1) % SPINNER.length
    spinchar = SPINNER[@spincount]

    return SPIN_BEGIN + spinchar + SPIN_END
  end

  # thanks to
  # http://github.com/cldwalker/hirb/blob/master/lib/hirb/util.rb#L61-71 [+]
  def detect_terminal_size
    if (ENV['COLUMNS'] =~ /^\d+$/) && (ENV['LINES'] =~ /^\d+$/)
      [ENV['COLUMNS'].to_i, ENV['LINES'].to_i]
    elsif (RUBY_PLATFORM =~ /java/ || !STDIN.tty?) 
#&& command_exists?('tput')
      [`tput cols`.to_i, `tput lines`.to_i]
    else
#command_exists?('stty') ? 
      `stty size`.scan(/\d+/).map { |s| s.to_i }.reverse 
#: nil
    end
    
    rescue
      nil
  end


end








class CLISpinBar < CLIBar
  # the buffer is for the cursor to sit.
  LINE_START = " "
  LINE_END   = " "

  BAR_START  = "["
  BAR_END    = "] "
  BAR_MARKER = "<=>"
  BAR_EMPTY  = " "

  def initialize(spinner=false, width=:fit, dev=$stdout)
    super(100, spinner, width, dev)
    @direction = 1
    
    set_value_callback(method("advance_spinner"))
  end

  def update 
    update_via_callbacks
  end

  # override to inject the 'spinning' callback
  def render_thread(sleeptime)
      raise "A thread is currently running.  Please stop it first using stop_thread()" if @thread

      @use_callbacks_for_updates = true #necessary for the spinner to spin
      @thread = Thread.new {start_render_loop(sleeptime)}
      return @thread
  end


  #[ <=>          ] 8,350       --.-K/s   in 0.007s  

  private

  def advance_spinner 
    @val += @direction
    @direction *= -1 if @val >= @max or @val<= 0

    return @val
  end


  def calculate_barstring
    @mutex.synchronize{
    # get optimum rendering width
    if @width == :fit  
      owidth = detect_terminal_size[0]
    else
      owidth = @width.to_i   
    end
 

    # will everything fit on screen? 
    bar_length = owidth - LINE_START.length - LINE_END.length
    if bar_length - @status.length < MIN_BAR_WIDTH
      owidth = @status.length
      return "#{LINE_START}#{@spinner ? get_spinner_string : ""}#{@status}#{LINE_END}"
      # if not, do not render bar at all, just show status
    end

    # we have already checked this.
    bar_length -= @status.length
  
    if @spinner and bar_length - (SPIN_BEGIN.length + SPIN_END.length + 1) > MIN_BAR_WIDTH # for the spin char.
      return "#{LINE_START}#{get_spinner_string}#{render_bar(bar_length - (SPIN_BEGIN.length + SPIN_END.length + 1))}#{@status}#{LINE_END}"
    end 
    return "#{LINE_START}#{render_bar(bar_length)}#{@status}#{LINE_END}"
    }
  end

  def render_bar(length)
    slack = length - BAR_END.length - BAR_START.length - BAR_MARKER.length
    marker_position = map_to_char( @val, @max,  slack)

    #puts "val: #{@val}, max: #{@max} slack: #{slack}, pos: #{marker_position}, length: #{length}"
    str = BAR_START
    str += (BAR_EMPTY * (marker_position))
    str += BAR_MARKER
    str += (BAR_EMPTY * (slack - marker_position))
    str += BAR_END
  end
end






class CLIProgressBar < CLIBar
  # the buffer is for the cursor to sit.
  LINE_START = " "
  LINE_END   = " "

  BAR_START  = "["
  BAR_END    = "] "
  BAR_MARKER = ">"
  BAR_FILL   = "="
  BAR_EMPTY  = " "


  def initialize(max=100, spinner=false, percentage=false, width=:fit, dev=$stdout)
    super(max, spinner, width, dev)
    set_percentage(percentage)
  end

  def set_percentage(percent)
    @mutex.synchronize{
      @percentage = (percent == true)
    }
  end

  private

  def calculate_barstring
    @mutex.synchronize{
    # get optimum rendering width
    if @width == :fit  
      owidth = detect_terminal_size
      owidth = owidth[0] if owidth != nil
    else
      owidth = @width.to_i   
    end
 
    owidth = 80 if owidth == nil

    # will everything fit on screen? 
    bar_length = owidth - LINE_START.length - LINE_END.length
    if bar_length - @status.length < MIN_BAR_WIDTH
      owidth = @status.length
      return "#{LINE_START}#{@spinner ? get_spinner_string : ""}#{@status}#{LINE_END}"
      # if not, do not render bar at all, just show status
    end

    bar_length -= @status.length

    if @spinner and bar_length - (SPIN_BEGIN.length + SPIN_END.length + 1) > MIN_BAR_WIDTH # for the spin char.
      return "#{LINE_START}#{get_spinner_string}#{render_bar(bar_length - (SPIN_BEGIN.length + SPIN_END.length + 1))}#{@status}#{LINE_END}"
    end 

    return "#{LINE_START}#{render_bar(bar_length)}#{@status}#{LINE_END}"
    }
  end

  def render_bar(length)
    slack = length - BAR_END.length - BAR_START.length - BAR_MARKER.length
  #- BAR_MARKER.length - BAR_START.length
    marker_position = map_to_char( @val, @max,  slack)

    #puts "#{marker_position + BAR_MARKER.length}, #{slack}"

    str = BAR_START
    str += (BAR_FILL * (marker_position))
    str += (marker_position == slack) ? (BAR_FILL * BAR_MARKER.length) : BAR_MARKER
    str += (BAR_EMPTY * (slack - marker_position))
    str += BAR_END

    if @percentage then
      #pstring = "|% 3.2f%%|" % ((100.0/@max.to_f) * @val.to_f)
      pstring = "|%.1f%%|" % ((100.0/@max.to_f) * @val.to_f)
      if slack > pstring.length
        insert_at = BAR_START.length + ((slack/2) - (pstring.length/2))
        return str[0..insert_at] + pstring + str[insert_at + 1 + pstring.length..-1]
      end
    end

    return str
    #puts "length: #{length}, #{str.length}"
  end
end


##test

## spinner, one motion per update 
#x = CLISpinBar.new(true)

## non-threaded operation
#300.times{|n|
  #x.update
  #x.set_status("#{n}")
  #x.render()
  #sleep 0.01
#}

## test of threaded update
#puts "non-blocking updates"
#sleep 1
#x.render_thread(0.01)

## allow the thread to run
#sleep 10
#x.stop_thread




#puts "Progress bar."
#sleep 1

#y = CLIProgressBar.new(1000, true, true)
#1001.times{|n|
  #y.update_abs(n)
  #y.set_status(n.to_s)
  #y.render
  ##("#{n} / 1000")
  #sleep 0.001
#}

#puts "non-blocking"
#sleep 1

#y.set_value_callback(lambda{ return rand(100) })
#y.set_status_callback(lambda{ return rand(100).to_s })
#y.set_use_callbacks(true)
#y.render_thread(0.1)

#sleep 5 
#y.set_spinner(false)
#y.set_use_callbacks(false)


#101.times{|n|
  #y.set_status("!CB: #{n*10}")
  #y.update_abs(n*10)
  #sleep 0.1
#}

#sleep 10 
#y.stop_thread
