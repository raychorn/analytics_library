#!/usr/bin/env python
# encoding: utf-8
"""
export_report.py

"""
import os
import re
import sys
import time
import utils
import getopt
import urllib
import urllib2
import datetime
import subprocess
import systemUser
import hadoop_mailer
from Export_Report import export_utils
from Export_Report import query_builder
from Export_Report import report_builder
#import MySQLdb

#Analytics Library Directory
analytics_lib = str(os.getenv("ANALYTICS_LIB"))
if analytics_lib is None:
	analytics_lib = "/home/hadoop/analytics/library"

# TODO pass as parameters
MYSERVER = 'localhost'
DBNAME = 'analytics'
USER = 'dbuser'
PASSWD = 'mxplay'

help_message = '''
Exports HDFS reports to different formats and from given date range, parameters, and
hive tables/events. Supports multiple joins and multiple keys by equality only. Can export
to CVS file, [SQL file, or directly into a MySQL table].

Usage Example:
$ python export_report.py -p 'event1.param1, event1.param2, event2.param3, event2.param1, event3.param' -k 'key1, key2' -o report.csv
$ python export_report.py --parameters 'applicationinfo.server_date, applicationinfo.client_id, deviceinstallation.xv' --keys 'client_id' --output report.csv
Usage: python export_report.py [OPTIONs...] 
Required Options: --parameters, --group

  -p, --parameters='event1.param1'     the parameter you want and in which event REQUIRED
      --group='bakrie'                 the groupid of the client: bakrie, sprintcm
  -k, --keys='key1,key2'               the keys used to associate if more than one event in -p DEFAULT:uuid
      --startdate='YYYY-MM-DD'         start date for gathering data DEFAULT:any
      --enddate='YYY-MM-DD'            end date for gathering data DEFAULT:any
  -t, --type='TYPE'                    TYPE can be 'csv' 'mysql(not this version)' DEFAULT:'csv'
  -o, --output='FILENAME'              the filename use for the report if exporting type is to a file DEFAULT:/home/ftpsecure/DATETIME
      --email='EMAIL'                  the EMAIL address that will be used for alerts (start, error, completion)
      --vendor_event='EVENT'           the name of the event where vendor information will be found.
      --report_name="REPORT"           the name of the custom report to execute, these are pre-built queries.
      --distinct='YES/NO'              option for unique output values, yes is UNIQUE values, no is ALL values DEFAULT:YES
      --raw                            if set, will bypass any aggregation on columns when generating to queries
      --scp                            if set, it will allow you to copy the file generated over the secure network using scp
                                       (--target_user, --target_server, --target_dir)
  -w, --where='CLAUSE'                 a WHERE Clause in HIVE Syntax ie: --where=event.col=\\'STRING\\'
  -v, --verbose[=WARNINGS]             enable verbose/debug mode 
      --version=VERSION                specify a version to use while creating reports
      --debug                          debug mode, do not run job but display query
'''	

class Usage(Exception):
  def __init__(self, msg):
    self.msg = msg
	
def email(subject, status, from_domain="localhost", to_email="", msg=""):
	if to_email is not None and to_email.strip() != '':
		hadoop_mailer.main(['hadoop_mailer.py', subject, status, from_domain, to_email, msg])
	return

def scp_method(email_addr, filename, target_user, target_server, target_dir): 
	print "Get in scp method\n"
	command = "scp"
	source = "/home/" + email_addr + "/csv/" + filename 
	target = target_user + "@" + target_server + ":" + target_dir  
	os.system(command + " " + source + " " + target)

#This function only exports the exact hive_command results into a file, 
#if parameters are passed, they are used to find alias for colunm names to write to top of file.		
def export_hive_to_file(hive_command, parameters = None, type = 'csv', outputname = None, ftpuser = 'ftpsecure', compress = True):
	#Create ftpuser if user does not already exist in the system
	#Default user information and password
	modifiedUser = ftpuser
	md5pass = 'whie1slac3'
	if ftpuser is not 'ftpsecure':
		modifiedUser, md5pass = systemUser.add(ftpuser)
		print modifiedUser, md5pass
	#Check outputname, if None then enter default value
	outputpath = "/home/" + modifiedUser + "/" + type + "/"
	if outputname is None:
		outputname = time.strftime("%Y%m%d%H%M%S", time.localtime())
	outputname = outputname + "." + type 
	#Check if 'hive' program exist on this system and assign the full path to location
	location = utils.whereis('hive')
	if location is not None:
		process = subprocess.Popen(['hive','-e',hive_command], shell=False, stdout=subprocess.PIPE)
		FILE = open(outputpath+outputname,"w")
		FILE.writelines(parameters + "\n")
		for line in process.stdout.xreadlines():
			FILE.writelines(line.replace('\N',''))
		FILE.close()
		process.poll()
		rtnCode = process.returncode
		if rtnCode is None:
			counter = 0
			while rtnCode is None or counter == 1000:
				process.poll()
				rtnCode = process.returncode
				counter += 1
			if counter == 1000:
				rtnCode = 7
		if int(rtnCode) == 0:
			print "Export Successfully Finished."
			if compress:
				process = subprocess.Popen(['zip', '-j', outputpath + outputname + '.zip',outputpath + outputname], shell=False, stdout=subprocess.PIPE)
				outputname = outputname + ".zip"
				print "File available: " + outputpath + outputname
			else:
				print "File available: " + outputpath + outputname
		else:
			print "Error: Return Code (" + str(rtnCode) + ")"
			raise NameError("Export Report was Unsuccessfull with Error Code: " + str(rtnCode) + ". Please forward this error to an administrator")
	else:
		#Raise Error
		raise NameError("Command 'hive' was not found anywhere on the system Path. Please make sure you are running export_report.py from the Master Node")
	return outputpath, outputname, modifiedUser, md5pass

#This function builds and exports based on the parameters given
def export_report(parameters, keys = 'uuid', range = [None,time.strftime('%Y-%m-%d')], type = 'csv', outputname = None, emailaddr = None, where = None, description = 'No description', distinct = 'Yes', groupid = 'default', ftpuser = 'ftpsecure', vendor_event = 'InstallationInfo', report_name = None, debug = False, raw_output = False, scp = False, target_user = None, target_server = None, target_dir = None): 
	try:
		COMMAND = ''
		#Build Query into COMMAND (Prefix: "hive --config $HIVE_HOME/conf -e \"" is added in function export_hive_to_file())
		COMMAND = "\"use " + groupid + "; "
		if report_name is not None:
			reportBuilder = report_builder.ReportBuilder(report_name,range,groupid)
			COMMAND += reportBuilder.getReportQuery()
		else:
			#Create QueryBuilder Object
			queryBuilder = query_builder.QueryBuilder()
			#Sanitize Parameters
			#Creates:
			###eventList - A List of Event Objects that have already been populated by sanatize parameters function.
			###parameters - A List of the original parameters requested in it's original order.
			###parameters_alias - A List of the aliases of the original parameters requested in it's order.
			eventList, parameters, parametersAliasList = export_utils.sanitize_parameters(parameters,groupid,raw_output)
			#Sanitize Where
			where, where_tables = export_utils.sanitize_where(where, parameters, groupid, eventList, vendor_event)
			#Add eventList to queryBuilder
			queryBuilder.addToEvents(eventList)
			COMMAND += queryBuilder.buildQuery(groupid,type,range,parameters,raw_output)
		COMMAND += "\""
		print
		print "Query:"
		print "-------------------------"
		print COMMAND
		print "-------------------------"
		print
		if debug == False:
			print "START TIME: [" + time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()) + "]" 
			print "START COMMAND: " + COMMAND
			outputpath, outputname, user, password = export_hive_to_file(COMMAND, utils.listToString(parametersAliasList), type, outputname, ftpuser)
			try:
				email("Report: Data Export Utility","complete!","smithmicro.com",emailaddr,"{'text':'email from export report', 'html':'<p>Your file is now complete. To access your report, click on this link: <a href=\"ftp://"+os.uname()[1]+"/"+type+"/"+outputname+"\">"+outputname+"</a> and enter your username and password below.</p><p>Username: <b>"+user.strip()+"</b><br />Password: <b>"+password.strip()+"</b></p>'}")
			except:
				print "The Email Server has Raised an Error"
			if bool(scp) == True: 	
				email_addr = user.strip()	
				scp_method(email_addr, outputname, target_user, target_server, target_dir)
	except IOError, e:
		print "I/O error(%(error0)s):(%(error1)s)" % { "error0":e[0],"error1":e[1] }
		email("Export_Report","failed","analytics.smithmicro.com",emailaddr,"\nError:\nI/O error("+utils.xstr(e[0])+"): "+xstr(e[1])+"\n"+utils.xstr(sys.exc_info()[0]))
	except NameError, e:
		print "An internal error has occured, possibly user input or system is temporarily unavailable."
		email("Export_Report","failed","analytics.smithmicro.com",emailaddr,"\nError:\nNameError("+utils.xstr(sys.exc_info()[0])+utils.xstr(e[0])+"): "+"An internal error has occured, possibly user input or system is temporarily unavailable. Please contact your administrator or SmithMicro at mv.DevAnalytics@smithmicro.com.")
		raise
	except:
		print "Unexpected Error: " + utils.xstr(sys.exc_info()[0])
		email("Export_Report","failed","analytics.smithmicro.com",emailaddr,"\nError:\n"+utils.xstr(sys.exc_info()[0]))
	print "\n+++++===HIVE END COMMAND===+++++++: " + COMMAND + "\n"
	return COMMAND
			
def main(argv=None):
  if argv is None:
    argv = sys.argv[1:]
  else:
    argv = argv.split()
  try:
    #print 'ARGV      :', argv[:]
    try:
      opts, args = 	getopt.getopt(argv, 'p:k:o:t:w:vdh', 
                                                   ['parameters=',
                                                    'keys=',
                                                    'startdate=','enddate=',
                                                    'email=',
                                                    'type=',
                                                    'help',
                                                    'output=',
                                                    'distinct=',
                                                    'where=',
                                                    'description=',
                                                    'group=',
                                                    'ftpuser=',
                                                    'vendor_event=',
                                                    'report_name=',
                                                    'verbose', 
                                                    'debug',
                                                    'raw',
                                                    'scp',
                                                    'target_user=',
                                                    'target_server=',
                                                    'target_dir=',
                                                    'version=',
                                                    ])
      print 'OPTIONS   :', opts
    except getopt.error, msg:
      raise Usage(msg)

    COMMAND = None
    # maps the event and keys
    # default keys
    keys = 'uuid'
    # date ranges
    startdate = None
    enddate = None
    # file output
    outputpath = ""
    outputname = None
    # output type
    outputtype = 'csv'
    # email address
    emailaddr = None
    # distinct values (unique - default: Yes)
    distinct = 'Yes'
    # custom where statement
    where = None
    description = 'No Description'
    groupid = 'default'
    ftpuser = 'ftpsecure'
    vendor_event = 'InstallationInfo'
    report_name = None
    verbose = False
    debug = False
    raw = False
    scp = False
    target_user = None
    target_server = None
    target_dir = None
    
    # option processing
    for option, value in opts:
      if option in ("-p", "--parameters"):
        parameters = value.strip()
        params = value.split(',')
      if option in ("-k", "--keys"):
        keys = value.strip()
      if option == '--startdate':
        startdate = value.strip()
        if len(startdate) == 0:
          startdate = None
      if option == '--enddate':
        enddate = value.strip()
        if len(enddate) == 0:
          enddate = None
      if option in ("-o", "--output"):
        outputname = value.strip()
      if option in ("-t", "--type"):
        outputtype = value.strip()
      if option == '--email':
        emailaddr = value.strip()
      if option == '--distinct':
        distinct = value.strip()
      if option in ("-w", "--where"):
        where = value.strip()
      if option in ("--group"):
        groupid = value.strip()
      if option in ("--ftpuser"):
        ftpuser = value.strip()
      if option in ("--vendor_event"):
        vendor_event = value.strip()
      if option in ("--report_name"):
        report_name = value.strip()
      if option in ("--description"):
        description = value.strip()
      if option in ("--raw"):
        raw = True
      if option in ("--scp"):
        scp = True
      if option in ("--target_user"):
        target_user = value.strip()
      if option in ("--target_server"):
        target_server = value.strip()
      if option in ("--target_dir"):
        target_dir = value.strip()
      if option in ("-v", "--verbose", "-d", "--debug"):
        verbose = True
        debug = True
      if option in ("-h", "--help"):
        raise Usage(help_message)

    # Command used to Export necessary Data
    COMMAND = export_report(parameters, keys, [startdate,enddate], outputtype, outputname, emailaddr, where, description, distinct, groupid, ftpuser, vendor_event, report_name, debug, raw, scp, target_user, target_server, target_dir)
    #if verbose == True:
    #  print COMMAND
    #  COMMAND = "echo " + COMMAND
    
  except Usage, err:
    print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
    print >> sys.stderr, "\t for help use --help"
    return 2


if __name__ == "__main__":
  sys.exit(main())
