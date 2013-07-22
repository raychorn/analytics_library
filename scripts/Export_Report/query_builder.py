#!/usr/bin/env python
# encoding: utf-8
"""
query_builder.py

"""
import os
import sys
import time
import event
import export_utils
sys.path.append("..")
import utils

"""This QueryBuilder Class uses a list of Event Objects to build a Final HQL Valid Query"""
#Constructor: List of Event Objects	
class QueryBuilder(object):
	queryCount = 0
	keyJoinCount = 0
	"""__init__() class constructor"""
	def __init__(self, events=None):
		#Attributes
		self.events = []
		#Assignments
		if events is not None:
			self.events = events
	"""__iter__() class iterator"""
	def __iter__(self):
		return self
	#This function appends the argument event to the event list for this class object
	#If event is a list of events it extends the list, otherwise it appends to it.
	def addToEvents(self,event):
		if isinstance(event,list):
			self.events.extend(event)
		else:
			self.events.append(event)
	#This function joins two events into one valid query that is later used for the final query
	#Events must be an Object of the type Event.
	def joinEvents(self,groupid,event1,event2,range=[None,time.strftime('%Y-%m-%d')],leftKeyTable=None,wrap=False):
		result = ""
		#Get Alias1 based on queryCount or leftKeyTable
		alias1 = "q" + str((self.queryCount))
		alias2 = "q" + str((self.queryCount)+1)
		if leftKeyTable is not None:
			alias1 = leftKeyTable
		#Build SubQuery/#Build Result, based on events passed. Maybe text or Event Class.
		subQ1 = event1
		if isinstance(event1,event.Event):
			subQ1 = event1.buildSubQuery(groupid,range)
			alias1 = event1.partition
			result = "(" + subQ1 + ") " + alias1
		elif 'JOIN' in subQ1:
			result = subQ1
		else:
			#Build SubQuery Alias1 based on queryCount
			alias1 = "q" + str((self.queryCount+1))
			self.queryCount += 1 
			result = "(" + subQ1 + ") " + alias1
		subQ2 = event2
		if isinstance(event2,event.Event):
			subQ2 = event2.buildSubQuery(groupid,range)
			alias2 = event2.partition
		else:
			#Build SubQuery Alias2 based on queryCount
			alias2 = "q" + str((self.queryCount+1))
			self.queryCount += 1
		#result = "select * from (" + subQ1 + ") " + alias1
		result += " JOIN (" + subQ2 + ") " + alias2
		result += " ON (" + (alias1+".uuid=") + (alias2+".uuid") + ")"
		return result
	#This function will build the objects in the event list into the final query
	#Optional event list can be pass instead of using this class' event list
	#Events List must be a List of Objects of the type Event.
	def buildQuery(self,groupid,type='csv',dsrange=[None,time.strftime('%Y-%m-%d')],paramOrderList=None,ignore_aggregates=False,leftKeyTable=None,leftKey=None,eventList=None):
		#Make sure index exist in event list. The first two events will be passed to the join
		#function, if other events are in the list, then consequent joins are created.
		#Check the leftKeyTable, if it is not None, then we need to rearrange the events so that the first index matches the leftKeyTable value.
		if leftKeyTable is not None:
			#Search for a match in self.events list
			item = None
			for event in self.events:
				if event.name == leftKeyTable:
					item = event
			#If a match was found
			if item is not None:
				#Remove match from list and append it to the front
				item.addToParameters(leftKey)
				
				if self.events[0].name != leftKeyTable:
					self.events.insert(0,self.events.pop(self.events.index(item)))
			else:
				#Nothing found, so table must be forced in list
				leftKeyEvent = event.Event(leftKeyTable,leftKeyTable,'uuid','uuid')
				leftKeyEvent.addToParameters(leftKey)
				self.events.insert(0,leftKeyEvent)
		#Index and Result Variables
		index = 0
		counter = 1
		result = None
		eventsLen = len(self.events)
		selected_params = ','.join(paramOrderList)
		#Holds the default transformation details, which is comma seperated columns
		transform = "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' USING '/bin/cat' AS dummy"
		#Holds any required JAR of FILE for the transformation to occur. Could be CSV.
		requires = ""
		if type == 'csv':
			requires = "add file " + utils.get_analytics_lib() + "/python_streaming/hive2csv.py; "
			transform = "USING 'hive2csv.py' AS dummy"
		while index < eventsLen:
			event = self.events[index]
			if (eventsLen is 1):
				result = "(" + event.buildSubQuery(groupid,dsrange) + ") " + event.name# + ") k"# + str((self.keyJoinCount+1))
			#If its not the very end, join with index+1
			elif (index < (eventsLen - 1)):
				result = "" + self.joinEvents(groupid,event,self.events[(index+1)],dsrange,) + ""
			else:
				result = "" + self.joinEvents(groupid,result,event,dsrange,) + ""
			index += 2
		result = requires + "select TRANSFORM(" + selected_params + ") " + transform + " from " + result + " "
		#Group By Keys, If there is a leftmost table and no uuid key, then we need to use it's UUID otherwise just the selected parameters
		if leftKeyTable is not None and ignore_aggregates == False:
			if (str(leftKeyTable) + "." + leftKey) not in paramOrderList:
				paramOrderList.insert(0,(str(leftKeyTable) + "." + leftKey))
			if (str(leftKeyTable) + "." + 'uuid') not in paramOrderList:
				paramOrderList.insert(0,(str(leftKeyTable) + ".uuid"))
			result += "group by " + ','.join(paramOrderList)
		elif ignore_aggregates == False:
			result += "group by " + selected_params
		return result
		
if (__name__ == '__main__') :
	eventList = [event.Event('logevents','event','col_name','columnname',False)]
	queryBuilder = QueryBuilder()
	queryBuilder.addToEvents(eventList)
	print
	print "QueryBuilder:"
	print "--query:"
	print queryBuilder.buildQuery('default','csv',[None,time.strftime('%Y-%m-%d')],["col_name"])
	print "--"
	print 'Done !'
	print