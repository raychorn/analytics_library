#!/usr/bin/env python
# encoding: utf-8
"""
export_utils.py

"""
import os
import re
import sys
import event
sys.path.append("..")
import utils

expression_function_list = [
'UPPER',
'LOWER'
]

selection_function_list = [
'SUM',
'AVG',
'COUNT',
'MIN',
'MAX'
]

#This function returns the alias of the parameter tuple passed (event.column)
#the alias is obtained from the group's event definition.
def get_parameter_alias(parameter,groupid):
	return get_parameter_definition(parameter,groupid)[1]
	
#This function returns the override function of the parameter tuple passed (event.column)
#the override function is obtained from the group's event definition.
def get_parameter_function(parameter,groupid):
	return get_parameter_definition(parameter,groupid)[0]
	
#This function returns the function and alias (if any) of the tuple passed (event.column)
#the function is a custom mathematical or string function to be applied by the event definition.	
def get_parameter_definition(parameter, groupid):
	ret_func = ""
	ret_alias = ""
	event, column = parameter.split('.',2)
	#Open the location of the event definition
	f = open(utils.get_analytics_lib()+"/events/"+str(groupid).strip()+"/"+str(event).strip(),'r')
	for line in f.xreadlines():
		#Get the correct column in the definition: (column_name,column_alias,column_func)
		col_name, col_alias, col_func, col_type = line.split(',',4)
		#Check the col_name from definition, if it's a digit prefix 'C_'
		if col_name.isdigit():
			col_name = "C_" + col_name
		#Check the col_name form the definition against the column name passed by parameter	
		if col_name.upper() == column.upper():
			#Found a match, grab the col_func and return it
			ret_func = col_func.strip()
			ret_alias = col_alias.strip()
			break
	f.close()
	#Check if an alias is avaiable, if not then make the alias equal column name
	if ret_alias == '':
		ret_alias = column
	return ret_func, ret_alias
	
#This function converts a parameter of the form event.column and wraps the custom
#fucntion provided by the event definition. This function is usually a mathematical
#or logical function like count, sum, avg, min, max or std deviation functions.
#Returns original_version, alias_version, and if a function was applied.
def apply_selection_function(parameter, groupid):
	new_parameter = parameter.strip()
	new_parameter_alias = ""
	is_changed = False
	#Get the function and alias to be apply based on the parameter
	function, alias = get_parameter_definition(new_parameter,groupid)
	new_parameter_alias = alias
	#If function is not None then we need to apply that function
	if function is not None or function.strip() != '':
		if function.upper() in selection_function_list:
			#new_parameter = "{function}({param})".format(function=function.upper(),param=new_parameter) #>py2.6
			new_parameter = "%(function)s(%(param)s)" % { 'function':function.upper(),'param':new_parameter }
			#new_parameter_alias = "{function}({param})".format(function=function.upper(),param=new_parameter_alias) #>py2.6
			new_parameter_alias = "%(function)s(%(param)s)" % { 'function':function.upper(),'param':new_parameter_alias }
			is_changed = True
	return new_parameter, new_parameter_alias, is_changed
	
#This function converts an expression of the form Key=Value to the
#respective expression function, ie. UPPER(Key)=UPPER(Value). 
#The respective function is obtained from the group's event definition.
def apply_expression_function(expression,groupid):
	new_expression = expression
	is_changed = False
	if '=' in expression:
		#split the expression max 2 times into key,value
		key, value = expression.split('=',2)
		#get the function to be applied that belongs to the key(parameter)
		function = get_parameter_function(key,groupid)
		#if function is not None then we need to apply that function
		if function is not None or function.strip() != '':
			if function.upper() in expression_function_list:
				#new_expression = "{function}({key})={function}({value})".format(function=function.upper(),key=key.strip(),value=value.strip()) #>py2.6
				new_expression = "%(function)s(%(key)s)=%(function)s(%(value)s)" % { 'function':function.upper(),'key':key.strip(),'value':value.strip() }
				is_changed = True
	return new_expression, is_changed
	
#This function sanitizes the parameter list, it makes sure that each
#parameter is in the form event.column, if it is not, it ignores it.
#Return [1]: List of EventObjects.
#Return [2]: List of parameters in the form of event.colname
#Return [3]: List of parameters with the alias name instead as well as any applied functions.
def sanitize_parameters(parameters,groupid,ignore_aggregates=False):
	#This list will hold objects of event class and will be passed to other functions to build the query
	eventList = []
	parameterList = []
	parameterAliasList = []
	new_parameters = ""
	new_parameters_alias = ""
	new_groupby_parameters = ""
	new_groupby_list = []
	params = parameters.split(',')
	counter = 1
	#Traverse the parameter list and make sure each parameter has the correct format
	for param in params:
		if '.' in param:
			#Get event and column name
			event_name, col_name = param.strip().split('.')
			if col_name not in table_columns(event_name, groupid):
				print "Column Not in table_columns: " + col_name
			#Append col_name to parameters list
			parameterList.append(param)
			#Append alias of param to parameter_alias list
			parameterAliasList.append(get_parameter_alias(param,groupid))
			if len(eventList) > 0:
				#Check if event is already in our event list
				exist = False
				for eventObject in eventList:
					if event_name in eventObject.name:
						#Add col_name to the object's parameter list
						eventObject.addToParameters(col_name)
						exist = True
				if exist == False:
					eventList.append(event.Event(event_name,event_name,col_name,get_parameter_alias(param,groupid),ignore_aggregates))
			else:
				#Create an event object and add it to the list
				eventList.append(event.Event(event_name,event_name,col_name,get_parameter_alias(param,groupid),ignore_aggregates))
		counter += 1
	return eventList, parameterList, parameterAliasList ###, new_groupby_parameters

#This function sanitizes the custom where clause, it reads the event definition
#for string functions and applies the function to a newly created where clause.
#The parameters arg is used to verify that any event/table used in the where clause
#actually exists within the parameters used in the select claused.
def sanitize_where(where, parameters, groupid, eventList=None, vendor_event='InstallationInfo'):
	new_where = ""
	new_where_tables = ""
	#Check if where is not None or empty, if it is, return None
	if where is not None and where.strip() != '':
		#Create Event Object #TODO: Remove Handcoded Tables/Partitions
		eventFilter = event.Event('logevents',vendor_event,None,None)
		eventFilter.addToWheres('en',"en='"+vendor_event+"'")
		new_where = ""
		#Split the where clause into an array of individual expressions. Key=Value.
		expression_list = re.split(r' \band\b | \bor\b ',where)
		counter = 1
		for expression in expression_list:
			#Check if event/table is being requested from any of the parameters, if not, then this will cause an error.
			#Split the expression to gather the table/event name and put it in a comma seperated list of new_where_tables used for creating the from JOINs
			if '.' in expression:
				#Get event/table name
				event_name = expression.split('.',1)[0]
				#Get column name
				column = (expression.split('.',2)[1]).split('=')[0]
				new_where_tables += event_name
				if counter != len(expression_list):
					new_where_tables += ","
			new_expression, is_changed = apply_expression_function(expression,groupid)
			new_where += new_expression
			#If it is not the end of the list add the Logical Statement TODO:Grab order from Original where to support other logical operators
			if counter != len(expression_list):
				new_where += " or "
			counter += 1
			'''For Event Class Where List'''
			#Latest Implementation only supports WHERE filters in a single EventObject
			eventFilter.addToWheres(column,new_expression)
		eventList.insert(0,eventFilter)
	else:
		new_where = None
	return new_where, new_where_tables
	
def table_columns(table, groupid):
	col_list = []
	f = open(utils.get_analytics_lib()+"/events/"+groupid+"/"+table,'r')
	for line in f.xreadlines():
		col = str(line.strip()).split(",",1)[0]
		if(col.isdigit()): #not in ("DeviceType","Device", "ByteMobile")):	
			col = "c_"+col
		else:
			re_float = re.compile('\d+(\.\d+)?')
			if re_float.match(col):
				col = "c_"+(str(col).replace(".","_"))
		col_list.append(col.strip())
	f.close()
	return col_list
	
#This function returns a Dictionary with table as Key and Columns as Values
def group_columns(groupid):
	#Map/Dictionary that holds the final return data
	columnMap = {}
	#Find all the files inside groupid folder
	files = os.listdir(utils.get_analytics_lib()+"/events/"+groupid)
	#Traverse through file list
	for inFile in files:
		print inFile
		#Ignore .svn folder/files
		if not inFile.startswith('.'):
			try:
				f = open(utils.get_analytics_lib()+"/events/"+groupid+"/"+inFile,'r')
				for line in f.xreadlines():
					col = str(line.strip()).split(",",1)[0]
					col_generic = None
					# Check if column is a float, has decimal value or
					# the value is an interger
					try:
						if(str(col).isdigit()):
							col = "c_"+col
						else:
							re_float = re.compile('\d+(\.\d+)?')
							if re_float.match(col):
								# If we have child columns, we should explicitly allow parent
								# to support selecting all of the child columns
								col_generic = "c_"+(str(col).split(".",1)[0])
								col = "c_"+(str(col).replace(".","_"))
					except:
						continue
					#if(str(col).isdigit()):
					#	col = "c_"+col
					if inFile in columnMap:
						if col_generic is not None and col_generic not in columnMap[inFile]:
							columnMap[inFile].append(col_generic)
						if col not in columnMap[inFile]:
							columnMap[inFile].append(col)
					else:
						columnMap[inFile] = [col]
				f.close()	
			except IOError, e:
				continue
	return columnMap

