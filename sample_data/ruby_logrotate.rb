#!/usr/bin/env/ruby

require 'fileutils'
require 'socket'
require 'rubygems'
require 'logrotate'

filename = ''
hostname = Socket.gethostname
hostname = hostname.sub(/[.]/, '_')
output_directory = '/tmp/processed'#RAILS_ROOT + '/log/processed'
if (!File.directory?(output_directory)) then Dir.mkdir(output_directory) end

lastaction = Proc.new() do
   `hadoop dfs -put #{filename} /user/hadoop/qlm8s/`
   #print `echo '#{filename}'`
end

options = {
  :gzip => false,
  :new => true,
  :date_time_ext => true,
  :date_time_format => "%Y%m%d_%T_#{hostname}_qlm8s",
  :directory => output_directory,
  :post_rotate => lastaction,
}

1.upto(3) do |iteration|

  FileUtils.touch("/tmp/erwin.dat")
  if (File.zero?("/tmp/erwin.dat"))
    result = LogRotate.rotate_file("/tmp/erwin.dat", options)

    print "=================================================\n"
    print "Iteration \##{iteration}\n"
    print "=================================================\n"
    #print result, "\n"
    print result.new_rotated_file, "\n"
    filename = result.new_rotated_file

    # Sleep for a short period so that next time rotate_file is called,
    # the current second will be different (and thus the rotated file
    # name will be different than the last).
    sleep(1.5)
  end
end

# clean up all files created by this example
#FileUtils.rm_f(Dir.glob('/tmp/erwin.dat.*'))
#FileUtils.rm_rf("/tmp/processed")