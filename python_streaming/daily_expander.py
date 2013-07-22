#!/usr/bin/env python
# encoding: utf-8
"""
daily_expander.py

Python Hadoop Streaming script.
"""
"""
TO DO 1: need to pass the log filename as a parameter
TO DO 2: parse_json is printing out 'u inside array lists ["ENU"] => "[u'ENU']"
"""

import sys, os, re
sys.path.append('.')
import urllib, simplejson
#import pbkdf2 #USED FOR SECURITY
#from binascii import hexlify, unhexlify
from geotagger import hook

#NOTE: SECURITY IS OFF (Strict Mode - Will not accept records with no security token)

#Security Constants
itercount = 1000
keylen = 48

# For 1.0
# eventlist = [
# 'ApplicationInfo',
# 'InstallationInfo',
# 'DeviceInsertion',
# 'Connection'
# ]

# For 1.1 # Deprecated
eventlist = [
'Connection',
'Disconnect',
'ConnectivityError',
'ConfigurationError',
'WiFiDeviceInsertion',
'4GDeviceInsertion',
'3GDeviceInsertion',
'EthernetDeviceInsertion',
'ApplicationInfo',
'InstallationInfo',
'DeviceInsertion'
]

def is_valid_event(event):
  if event in eventlist:
    return True
  return False

def check_field_type(field):
  if field.isdigit():
    return "DIGIT"
  if len(field.split('.')) == 4:
    return "IP"
  return False

def parse_json(definition, json):
	result = {}
	result.update(definition)
	for k in definition.keys():
		try:
			if isinstance(json[k], (basestring,int,float)):
				#result[k] = str(k) + ":" + str(json[k])
				result[k] = str(json[k])
			elif isinstance(json[k], (list)):
				#result[k] = '"' + str(k) + ":" + str(json[k]) + '"'
				result[k] = '"' + str(json[k]) + '"'
			else:
				result.update(parse_json(result,json[k]))
		except:
			pass
	return result

#try:
#  filepath = os.environ["map_input_file"] 
#  filename = os.path.split(filepath)[-1]
#except KeyError:
  # in testing...
#  filename = 'events.log'
'''Get the Unique Date and Events Available in this Log'''      
def mapper1(args):
  '''
  format before mapper:
  date\tip\tjson{} or date\tversion\ttoken\json{}
  map to the following format:
  date\tevent\n
  '''
  for line in sys.stdin: 
	try:
	  # remove leading and trailing whitespace
	  line = line.strip()
	  # split the line, max 5 splits
	  fields = line.split("\t",5)
	  # check if the second field is a version number or ip
	  #field_type = check_field_type(fields[1])
	  #if field_type == "DIGIT": #Check the digit with security token
	  json = simplejson.loads(fields[4])
	  #################################################################
	  ## UNCOMMENT TO TURN ON SECURITY (don't want a flag)
	  ##password = json['d'][4]
	  ##salt = unhexlify(fields[2][:16])
	  ##expected = fields[2][16:]
	  ##ret = pbkdf2.pbkdf2( password, salt, itercount, keylen )
	  ##hexret = hexlify(ret)
	  ##if hexret != expected:
		##  raise Exception("security check failed")
	  ## UNCOMMENT - END
	  #################################################################
	  #elif field_type == "IP": #No Security Token Available
		#json = simplejson.loads(fields[2])
	  #else:
		#raise Exception("not a valid format")
	  # clean and get only the date, discard time.
	  date = fields[0].split(" ")[0]
	  event = json['d'][5]
	  # Allow Any Event Name
	  #if is_valid_event(event):
	  sys.stdout.write('%s}%s\n' % (date, event.strip()) )
	except:
	  pass

def reducer1(args):
  '''
  reduce to the following format:
  unique(date\tevent)\n
  '''
  # maps the date and event
  unique_values = {}
  for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    #date, event = line.split('\t')
    try:
      unique_values[line] = []
    except:
      # skip bad rows
      pass     
  for k in unique_values.keys():
    print '%s' % (k.strip())   

'''Get the Event Line and Expand it based on Passed Argument'''      
# TODO: The following set of Map/Reduce can be optimized to be a
#       mapper-only function. We are going overkill introducing a
#       reducer during this phase as there is no aggregation neeeded
#       and thus we should take advantage of this in the mappers.
def mapper2(definition, date_event, args):
  '''
  format before mapper:
  date\tip\tjson{} or date\tversion\ttoken\tip\tjson{}
  map to the following format:
  date\tip\tevent\json[]\n
  '''
  date_event = date_event.strip()
  search_date, search_event = date_event.split('}')
  for line in sys.stdin: 
	try:
	  # remove leading and trailing whitespace
	  line = line.strip()
	  # split the line, max 5 splits
	  fields = line.split("\t",5)
	  # clean and get only the date, discard time.
	  date_time = fields[0]
	  date = date_time.split(" ")[0]
	  if date == search_date:# and date or search_date is not None:
		json = simplejson.loads(fields[4])
		ip = fields[3]
		# check if the second field is a version number or ip
		#field_type = check_field_type(fields[1])
		#if field_type == "DIGIT": #Check the digit with security token later
		#	json = simplejson.loads(fields[4])
		#	ip = fields[3]
		#elif field_type == "IP": #No Security Token Available
		#	json = simplejson.loads(fields[2])
		#	ip = fields[1]
		#else:
		#	raise Exception("not a valid format")
		event = json['d'][5]
		if event == search_event:# and event or search_event is not None:
			#if field_type == "DIGIT": #Check the digit with security token
			#################################################################
		  	## UNCOMMENT TO TURN ON SECURITY (don't want a flag)
			##password = json['d'][4]
			##salt = unhexlify(fields[2][:16])
			##expected = fields[2][16:]
			##ret = pbkdf2.pbkdf2( password, salt, itercount, keylen )
			##hexret = hexlify(ret)
			##if hexret != expected:
			##	raise Exception("security check failed")
			## UNCOMMENT - END
			#################################################################
                        ##~ Sim-Reducer
                        # The following was moved from the Reducer to take advantage of the
                        # performance a mapper-only job since a reducer is not needed.
                        result = {}
                        output = ''
                        geotags = ''
                        try:
                            log_format = json['d'][0]
                            client_date = json['d'][1]
                            project_id = json['d'][2]
                            version = json['d'][3]
                            uuid = json['d'][4]
                            ext_log = simplejson.dumps(json['d'][6])
                            ext_json = simplejson.loads(ext_log)
                            result = parse_json(definition,ext_json)
                            for k in sorted(result.iterkeys()):
                                output = output + str(result[k]) + '\t' 
                            # The Following is a Hack while we create a Filter Framework for Columns
                            # It will add 5 additional columns to support the Geotagger Hooks/Heat Values
                            try:
                                lon = float(result['139'])
                                lat = float(result['140'])
                                if isinstance(lon, (float)) and isinstance(lat, (float)):
                                    hooks = hook.geotagger(lat, lon)
                                    geotags = (str(int(hooks['heat_lat'])) + '\t' +
                                            str(int(hooks['heat_lng'])) + '\t' +
                                            str(int(hooks['heat_x'])) + '\t' +
                                            str(int(hooks['heat_y'])) + '\t' +
                                            str(int(hooks['heat_num'])) + '\t' +
                                            str(hooks['heat_gps']) + '\t')
                            except:
                                pass
                            sys.stdout.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%s\n' % (date_time, ip, log_format, client_date, project_id, version, uuid, output, geotags) )
                        except:
                            pass
                        ##~
			#sys.stdout.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (date_time, ip, json['d'][0], json['d'][1], json['d'][2], \
			#  json['d'][3], json['d'][4], simplejson.dumps(json['d'][6])) )
	except:
	  pass

#reducer not needed - deprecated
#def reducer2(definition, args):
#  '''
#  reduce to the following format:
#  \t
#  '''
#  for line in sys.stdin:
#    #sys.stdout.write('%s' % "test")
#    result = {}
#    # remove leading and trailing whitespace
#    line = line.strip()
#    output = ''
#    geotags = ''
#    #sys.stdout.write('%s\n' % (line))
#    try:
#      date,ip,log_format,client_date,project_id,version,uuid,ext_log = line.split("\t")
#      ext_json = simplejson.loads(ext_log)
#      result = parse_json(definition,ext_json)
#      for k in sorted(result.iterkeys()):
#        output = output + str(result[k]) + '\t'
#      # The Following is a Hack while we create a Filter Framework for Columns
#      # It will add 5 additional columns to support the Geotagger Hooks/Heat Values
#      try:
#        lon = float(result['139'])
#        lat = float(result['140'])
#        if isinstance(lon, (float)) and isinstance(lat, (float)):
#          hooks = hook.geotagger(lat, lon)
#          geotags = (str(int(hooks['heat_lat'])) + '\t' +
#                    str(int(hooks['heat_lng'])) + '\t' +
#                    str(int(hooks['heat_x'])) + '\t' +
#                    str(int(hooks['heat_y'])) + '\t' +
#                    str(int(hooks['heat_num'])) + '\t' +
#                    str(hooks['heat_gps']) + '\t')
#      except:
#        pass
#      sys.stdout.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%s\n' % (date, ip, log_format, client_date, project_id, version, uuid, output, geotags) )
#    except:
#      # skip bad rows
#      pass
      
if __name__ == "__main__":
  if len(sys.argv) == 1:
    print "Running sample data locally..."
    # Step 1
    os.system('cat ../sample_data/events.fred.sampler | '+ \
    ' ./daily_expander.py mapper1 | LC_ALL=C sort |' + \
    ' ./daily_expander.py reducer1 > tmp_dates_and_events.txt')
	# Step 2  
    counter = 0
    f = open('./tmp_dates_and_events.txt','r')
    for line in f.xreadlines():
      date_event = line.strip()
      date,event = date_event.split('}')
      os.system('cat ../sample_data/events.fred.sampler | '+ \
      ' ./daily_expander.py mapper2 ' + line.strip() + ' ' + str(os.getenv("ANALYTICS_LIB"))+'/events/bakrie/logevents' + ' > part-0000' + str(counter) + '_' + date + '_' + event) # \
      #' ./daily_expander.py reducer2 ' + line.strip() + ' ' + str(os.getenv("ANALYTICS_LIB"))+'/events/bakrie/logevents' + ' > part-0000' + str(counter) + '_' + date + '_' + event)
      counter += 1
    f.close()
    os.system('head part-0000')    
  elif sys.argv[1] == "mapper1":
    mapper1(sys.argv[2:])
  elif sys.argv[1] == "reducer1":
    reducer1(sys.argv[2:]) 
  elif sys.argv[1] == "mapper2":
    definition = {}
    # This is where we map all the different parameters the we accept inside of the JSON,
    # we call the file that holds these values (future: open mysql table) and then read them
    # line by line and form a python dictionary that we pass to the reducer2 which gets to the parser.
    #event_to_open = sys.argv[2].split('}')[1]
    event_definition_file = sys.argv[3]
    #/home/hadoop/analytics-thirdparty/lib/events/ TODO: MySQL will resolve environment issues
    #f = open('/home/hadoop/analytics-thirdparty/lib/events/'+event_to_open.lower(),'r')
    # TODO: Paths *NEED* to be hardcoded, every machine needs access, future solutions:
    # MySQL will resolve environment issues or NFS directory where all machines can reference
    # this code is copied and then sent over the cluster, each need to be pointing to the same locations.
    f = ''
    try:
    	f = open(event_definition_file,'r')
    	#f = open('/home/hadoop/analytics-thirdparty/lib/events/bakrie/logevents','r') # TO DO: pass the filename as a parameter
    except:
    	try:
    		#f = open('/home/hadoop/analytics/library/events/bakrie/logevents','r') # TO DO: pass the filename as a parameter
    		f = open('/Users/lramos/Development/SVN/analytics/trunk/analytics_library/events/SprintMND/logevents','r')
    	except:
    		f = open('/home/hadoop/analytics/library/events/default/logevents','r') # TO DO: pass the filename as a parameter
    #Get the first column of each event's parameter and put it in a map 
    for line in f.xreadlines():
      definition[str((line.split(',',1)[0]).strip())] = ''
    f.close()
    mapper2(definition, sys.argv[2], sys.argv[3:])
#  elif sys.argv[1] == "reducer2":
#    definition = {}
#    # This is where we map all the different parameters the we accept inside of the JSON,
#    # we call the file that holds these values (future: open mysql table) and then read them
#    # line by line and form a python dictionary that we pass to the reducer2 which gets to the parser.
#    #event_to_open = sys.argv[2].split('}')[1]
#    event_definition_file = sys.argv[3]
#    #/home/hadoop/analytics-thirdparty/lib/events/ TODO: MySQL will resolve environment issues
#    #f = open('/home/hadoop/analytics-thirdparty/lib/events/'+event_to_open.lower(),'r')
#    # TODO: Paths *NEED* to be hardcoded, every machine needs access, future solutions:
#    # MySQL will resolve environment issues or NFS directory where all machines can reference
#    # this code is copied and then sent over the cluster, each need to be pointing to the same locations.
#    f = ''
#    try:
#    	f = open(event_definition_file,'r')
#    	#f = open('/home/hadoop/analytics-thirdparty/lib/events/bakrie/logevents','r') # TO DO: pass the filename as a parameter
#    except:
#    	try:
#    		#f = open('/home/hadoop/analytics/library/events/bakrie/logevents','r') # TO DO: pass the filename as a parameter
#    		f = open('/Users/lramos/Development/SVN/analytics/trunk/analytics_library/events/SprintMND/logevents','r')
#    	except:
#    		f = open('/home/hadoop/analytics/library/events/default/logevents','r') # TO DO: pass the filename as a parameter
#    #Get the first column of each event's parameter and put it in a map 
#    for line in f.xreadlines():
#      definition[str((line.split(',',1)[0]).strip())] = ''
#    f.close()
#    reducer2(definition, sys.argv[2:])


      
