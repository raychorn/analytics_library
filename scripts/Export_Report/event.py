#!/usr/bin/env python
# encoding: utf-8
"""
event.py

"""
import os
import sys
import time
import export_utils
sys.path.append("..")
import utils

"""This Event Class holds individual tables/events requested by the user"""
#Constructor: Name of event (ie installationinfo), Single Parameter Column (ie c_70), Alias of Parameter (ie wimaxcount), Whole where clause for event.
class Event(object):
	"""__init__() class constructor"""
	def __init__(self, name=None, partition=None, parameter=None, alias=None, ignore_aggregates=False, where=None):
		#Attributes
		self.parameters = []
		self.aliases = []
		self.wheres = []
		self.wheresMap = {}
		self.groupByList = []
		#Assignments
		self.name = name
		self.partition = partition
		self.ignore_aggregates = ignore_aggregates
		if parameter is not None:
			self.parameters.append(parameter)
		if alias is not None:
			self.aliases.append(alias)
		if where is not None:
			self.wheres.append(where)
	"""__iter__() class iterator"""
	def __iter__(self):
		return self
	#This function appends the argument parameter to the paramter list for this class object
	def addToParameters(self,parameter):
		self.parameters.append(parameter)
		setone = set(self.parameters)
		self.parameters = list(setone)
	#This function appends the argument where to the where list for this class object
	#The where list is a list of dictionary map objects, with the key being the col_name
	#so we can group these by key and apply correct logical operators and parenthesis around them.
	def addToWheres(self,key,where):
		whereMap = {}
		if key in self.wheresMap:
			self.wheresMap[key].append(where)
		else:
			self.wheresMap[key] = [where]
		self.wheres.append(where)
	#This function returns a list of parameters tuples event.column 
	#(list is comma seperated with it's applied functions [aggregates] and name)
	def returnParametersAsList(self,groupid,ignore_aggregates=False):
		parameterList = ""
		counter = 1
		#Traverse through each parameter in object's list
		for param in self.parameters:
                        new_param = utils.xstr(self.name+"."+param) 
                        is_changed = False
                        if self.ignore_aggregates == False:
                            #Apply any functions to the parameter, ie SUM, AVG, etc.
                            new_param, alias, is_changed = export_utils.apply_selection_function(utils.xstr(self.name+"."+param),groupid)
			#For subqueries it is nessesary to create an alias for the aggregation function
			if is_changed:
				new_param += " as " + param
			#Concatenate to parameterList and add a comma if not the end of the list
			parameterList += new_param
			if counter != len(self.parameters):
			 	parameterList += ","
			counter += 1
                if 'uuid' not in self.parameters:
			if parameterList is not "":
				parameterList += "," + self.name+".uuid"
			else:
				parameterList += self.name+".uuid"
		return parameterList
	#This function returns a list of groupby parameters 
	#(list is comma seperated and does not include any aggregates)
	def returnGroupByAsList(self,groupid,ignore_aggregates=False):
		groupbyList = ""
		#Traverse through each parameter in object's list
		for param in self.parameters:
                        is_changed = True
                        if self.ignore_aggregates == False:
                            #Apply any functions to the parameter, ie SUM, AVG, etc.
                            is_changed = export_utils.apply_selection_function(utils.xstr(self.name+"."+param),groupid)[2]
			#If no change was made by function, then it belongs in the groupby list
			if not is_changed and param not in self.groupByList:
				self.groupByList.append(param)
		#Groupby Required Parameters - make sure they are there
		if 'uuid' not in self.groupByList:
                    self.groupByList.append('uuid')
		#Traverse through the new groupby list, this is the original parameter list, without parameters with math functions
		counter = 1
		for groupby_param in self.groupByList:
			groupbyList += self.name+"."+groupby_param
			if counter != len(self.groupByList):
				groupbyList += ","
			counter += 1
		return groupbyList
	#This function will build the subquery based on this object attributes
	#Format: select [parametersAsList] from [name] [where] [groupbyAsList]
	def buildSubQuery(self,groupid,range=[None,time.strftime('%Y-%m-%d')],ignore_aggregates=False):		
		#Parameters used in Subquery select Clause
		subParameters = self.returnParametersAsList(groupid)
		subGroupBy = self.returnGroupByAsList(groupid)
		#Required parameters for a subquery to do joins (date ranges are done internally)
		#Build Date Ranges for the where clause
		#Check DATES are valid, if not use default
		if not utils.is_valid_date(utils.xstr(range[0])):
			range[0] = ''
		if not utils.is_valid_date(utils.xstr(range[1])):
			range[1] = time.strftime('%Y-%m-%d')
		subDateRange = self.name + ".ds>='" + utils.xstr(range[0]).split(" ")[0] + "' and " + self.name + ".ds<='" + utils.xstr(range[1]).split(" ")[0] + "'"
		#Check if DATES have time, if so append server_date.
		if utils.has_valid_time(utils.xstr(range[0])):
			subDateRange += " and " + self.name + ".server_date>='" + utils.xstr(range[0]) + "'"
		if utils.has_valid_time(utils.xstr(range[1])):
			subDateRange += " and " + self.name + ".server_date<='" + utils.xstr(range[1]) + "'" 
		subWhere = " where " + subDateRange
		if len(self.wheres) > 0:
			logicOp= " and "
		'''for where in self.wheres:
			subWhere += logicOp
			subWhere += where
			logicOp = " or "
		'''
		for key in self.wheresMap.keys():
			subWhere += logicOp
			counter = 1
			subWhere += "("
			for where in self.wheresMap[key]:
				logicOp = " or "
				subWhere += where
				if counter != len(self.wheresMap[key]):
					subWhere += logicOp
				counter += 1
			subWhere += ")"
			logicOp = " and "
                query = "select " + subParameters + " from " + utils.xstr(self.name) + subWhere
                # if ignore aggregates is not nessesary then include the groupby parameters
                if self.ignore_aggregates == False:
                    query = query + " group by " + subGroupBy 
                return query

if (__name__ == '__main__') :
	event = Event('logevents','event','col_name','columnname',False)
	print
	print "Event Information:"
	print event.name
	print event.returnParametersAsList('default')
	print "--query:"
	print event.buildSubQuery('default')
	print "--"
	print 'Done !'
	print