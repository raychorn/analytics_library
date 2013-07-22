#!/usr/bin/env python
# encoding: utf-8
import os
import sys
import time
import simplejson 
import string
import getopt
import export_report as export
import hadoop_mailer
from Export_Report import export_utils

def call_method(str_params):
    params = {}
    #raise  
    #str_params = "{\"debug\":\"True\",\"language\":\"python\",\"filename\":\"export_report.py\",\"report_name\":\"report_007\",\"columns\":\"6,7,8,9,10\"}" 
    params = simplejson.loads(str_params)
    if params["language"] in ("python"):
        if (params["filename"] in ("export_report.py")):
        		parameters, keys, start_date, end_date, outputtype, outputname, email, where, description, distinct, vendor, project, vendor_event, report_name, debug, israw, scp, target_user, target_server, target_dir = export_params_handler(params)
        		export.export_report(parameters, keys, [start_date,end_date], outputtype, outputname, email, where, description, distinct, project, email, vendor_event, report_name, debug, israw, scp, target_user, target_server, target_dir)  # outputname is title

def prefix_columns(columns,project='default'):
	resultTokens = []
	columns = columns.strip().strip(',')  
	tokens = columns.split(",")
	
	#convert String to int for sorting 
	count = len(tokens) - 1
	while (count >= 0):
		if tokens[count].isdigit():
			tokens[count] = int(tokens[count])
		count = count - 1;
	#sort the tokens - int sorted is different with string sorted
	tokens = sorted(tokens)
	
	count = len(tokens) - 1

	#tableList will be use to keep a list of tables that had a match
	tableList = []
	#This is a dictionary of acceptable columns for the analytics system.
	#It calls export_report.group_columns() which returns a map of table keys,
	#and a value of type list with the different columns available.
	acceptable_columns = export_utils.group_columns(project)
	#traverse through the map and then the tokens from the parameter columns.
	for k, v in acceptable_columns.iteritems():
		if k not in 'connection' and k not in '_common':
			for token in tokens:
				#if token is a digit, then prefix c_
				if (str(token).isdigit()):
					token = "c_"+str(token)
				#if token is found within the column list add the key to tableList
				#and append the concatinated token to the result list.
				if token in v:
					resultTokens.append(k+"."+token)
					tableList.append(k)
	#traverse throught the tokens to find all the common fields to choose a table from tableList
	for token in tokens:
		if token in acceptable_columns['_common']:
			resultTokens.append(tableList.pop()+"."+token)
				
	return ",".join(resultTokens) 

# In charged of fetching all parameters that begin with c_, these parameters if they have
# a value assign to them, will be converted to c_# = 'value' and inserted into a where clause
def where_params(params_keys, params, project='default'):
    c_list = []
    c_index = 0
    where_params = []
    where_params_str = "" 
    index = len(params_keys) - 1
    while (index >= 0):
	params_keys[index] = params_keys[index].strip()
	if(params_keys[index].startswith("c_")):
		c_list.append(params_keys[index])
	index = index - 1
    c_index = len(c_list) - 1

    table_names = ["logevents"]
    logevents_params_list =  export_utils.table_columns(table_names[0],project)

    while (c_index >= 0):
	c_key = c_list[c_index]
	c_value = params[c_key]
       	tokens = c_value.split(",")
   	count = len(tokens)
        if(logevents_params_list.count(c_key) > 0):
                c_key = table_names[0] + "." + c_key
	# end of prefix table names	
	if(count > 1):
		where_params = params_seperate(where_params, c_key, tokens, count)
       	else:
		#where_params.append("UPPER(" + c_key + ")=UPPER(\'" + c_value + "\')") # -luis temp
		where_params.append(c_key + "=\'" + c_value + "\'") # for call method directly
		#where_params.append(c_key + "=\\\'" + c_value + "\\\'") # for os.system
   	c_index = c_index - 1 
    p_index = len(where_params) - 1
    while(p_index > 0):
	where_params_str = where_params_str + where_params.pop() + " or "
	p_index = p_index - 1
    if (p_index == 0): 
	where_params_str = where_params_str + where_params.pop()
    #print "where_params_str =====> " + where_params_str + "\n"					
    return where_params_str			

def params_seperate(where_params, c_key, tokens, count):
	#print "%s **** %s" %(where_params, tokens) 
	index = count - 1
	while(index >= 0):
		# handle special characters inside the parameters
		tokens[index] = tokens[index].replace("'", "\\'")	
		# print "======>%s\n" %(tokens[index]) 	
		#next 3 lines added by luis - temporarily
		#if c_key.split('.')[1] == 'c_48':
		#	where_params.append("UPPER(" + c_key + ")=UPPER(\'" + tokens[index] + "\')") # for call method directly
		#else:
		where_params.append(c_key + "=\'" + tokens[index] + "\'") # for call method directly
		#where_params.append(c_key + "=\\\'" + tokens[index] + "\\\'") # for os.system
		index = index - 1
	return where_params		


def export_params_handler(params):
    params_keys = params.keys()
    if(("project" not in params_keys) or (params["project"] == "")):
        project = 'default'
    else:
        project = params["project"]
    if('columns' not in params_keys):
   	columns = ""
    else:
    	columns = params["columns"]
	columns = prefix_columns(columns,project)
	print "columns ===> " + columns + "\n" 
    if(("keys" not in params_keys) or (params["keys"] == "")):
    	keys = 'uuid'
    else:
    	keys = params["keys"]
    if(("start_date" not in params_keys) or (params["start_date"] == "")):
    	start_date = None
    else:
    	start_date = params["start_date"]
    if(("end_date" not in params_keys) or (params["end_date"] == "")):
    	end_date = None
    else:
    	end_date = params["end_date"]
    if(("outputtype" not in params_keys) or (params["outputtype"] == "")):
    	outputtype = 'csv'
    else:
    	outputtype = params["outputtype"]
    #if(("outputname" not in params_keys) or (params["outputname"] == "")):
   # 	outputname = None
   # else:
   # 	outputname = params["outputname"]
    if(("title" not in params_keys) or (params["title"] == "")):
        outputname = None
    else:
        outputname = params["title"]
   	outputname = outputname.replace("%20", "_") 
   	outputname = outputname.replace(" ", "_") 
	if(len(outputname) > 50):
		outputname = output[0:50]
    if(("description" not in params_keys) or (params["description"] == "")):
        description = 'No description'
	description = description.replace("%20", " ") 
    else:
        description = params["description"]
    if(("email" not in params_keys) or (params["email"] == "")):
    	email = None
    else:
    	email = params["email"]
    if(("vendor_event" not in params_keys) or (params["vendor_event"] == "")):
        vendor_event = 'InstallationInfo'
    else:
        vendor_event = params["vendor_event"]
    if(("report_name" not in params_keys) or (params["report_name"] == "")):
    	report_name = None
    else:
    	report_name = params["report_name"]
    if(("distinct" not in params_keys) or (params["distinct"] == "")):
    	distinct = 'Yes' 
    else:
    	distinct = params["distinct"]
    #if(("vendor" not in params_keys) or (params["vendor"] == "")):
    if(("c_48" not in params_keys) or (params["c_48"] == "")):
        vendor = False
    else:
        vendor = params["c_48"]
    if(("israw" not in params_keys) or (params["israw"] == "")):
        israw = False
    else:
        israw = params["israw"]
    if(("scp" not in params_keys) or (params["scp"] == "")):
        scp = False
    else:
        scp = params["scp"]
    if(("target_user" not in params_keys) or (params["target_user"] == "")):
        target_user = None
    else:
        target_user = params["target_user"]
    if(("target_server" not in params_keys) or (params["target_server"] == "")):
        target_server = None
    else:
        target_server = params["target_server"]
    if(("target_dir" not in params_keys) or (params["target_dir"] == "")):
        target_dir = None
    else:
        target_dir = params["target_dir"]
    if(("debug" not in params_keys) or (params["debug"] == "")):
        debug = False
    else:
        debug = params["debug"] 
    # if("where" not in params_keys):
    #   where = None
    # else:
    #   where = params["where"]
    where = where_params(params_keys, params, project) 

    return columns, keys, start_date, end_date, outputtype, outputname, email, where, description, distinct, vendor, project, vendor_event, report_name, debug, israw, scp, target_user, target_server, target_dir

def main(argv=None):
	try:
		str_params = str(sys.argv[1])	
		print "str_params = " + str_params
		call_method(str_params) 
	except:
		err_msg = "Oops! Error at main in amqp_consumer_sub.py ! Error is: " + str(sys.exc_info()[0]) 
		print err_msg

if __name__ == "__main__":
  main()
  #sys.exit(main())

