#!/usr/bin/env python
# encoding: utf-8
"""
report_builder.py

"""
import os
import sys
import time
sys.path.append("..")
import utils

"""This ReportBuilder Class Uses Pre-Built Queries in the Sytem to Generate Report Valid HQL"""
#Constructor: Name of report.
class ReportBuilder(object):
	"""__init__() class constructor"""
	def __init__(self, name=None, range=[None,time.strftime('%Y-%m-%d')], groupid='default', type='FileSystem', query=None):
		#Attributes
		#Assignments
		self.name = name
		self.query = query
		self.range = range
		self.groupid = groupid
		self.type = type
	"""__iter__() class iterator"""
	def __iter__(self):
		return self
	#This function returns the location of the report
	def getReportLocation(self):
		return "location path"
	#This function gets the hql query of the report
	def getReportQuery(self,range=None,groupid=None,type=None):
		#return "report query from file/db"
		if range is None:
			range = self.range
		if groupid is None:
			groupid = self.groupid
		if type is None:
			type = self.type
		self.__fetchQueryFromLocation(groupid,type)
		self.__parseQueryDateRanges(range)
		return self.query
	#This function parses date variables with input values
	def __parseQueryDateRanges(self,range):
		if not utils.is_valid_date(utils.xstr(range[0])):
			range[0] = ''
		if not utils.is_valid_date(utils.xstr(range[1])):
			range[1] = time.strftime('%Y-%m-%d')
		if utils.has_valid_time(utils.xstr(range[0])):
			start_date, start_time = range[0].split(" ")
		else:
			start_date = range[0]
			start_time = ''
		if utils.has_valid_time(utils.xstr(range[1])):
			end_date, end_time = range[1].split(" ")
		else:
			end_date = range[1]
			end_time = '23:59:59'
		query = self.query
		query = query.replace("#STARTDATE",start_date)
		query = query.replace("#STARTTIME",start_time)
		query = query.replace("#ENDDATE",end_date)
		query = query.replace("#ENDTIME",end_time)
		self.query = query
	def __fetchQueryFromLocation(self,groupid,type):
		#TODO: Case Type for FileSytem/MySQL/URL
		try:
			f = open(utils.get_analytics_lib()+"/reports/"+str(groupid).strip()+"/"+self.name+".sql",'r')
			self.query = f.read()
			f.close()
		except IOError:
			raise NameError("The report trying to be fetch is not available.")
		
if (__name__ == '__main__') :
	reportBuilder = ReportBuilder()
	reportBuilder.name = "report_007"
	reportBuilder.range = ['2011-07-04 00:00:00','2011-07-04 23:59:59']
	print
	print "ReportQuery Information:"
	print reportBuilder.name
	print "--query:"
	print reportBuilder.getReportQuery()
	print "--"
	print 'Done !'
	print