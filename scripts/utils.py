#!/usr/bin/env python
# encoding: utf-8
"""
utils.py

"""
import os
import time

def get_analytics_lib():
	analytics_lib = xstr(os.getenv("ANALYTICS_LIB"))
	if analytics_lib is None or analytics_lib.strip() == '':
		analytics_lib = "/home/hadoop/analytics/library"
	return analytics_lib

# Behaves like str() but instead returns empty string if s is None
def xstr(s):
	if s is None:
		return ''
	return str(s)
	
# Use: To find the full path of a program in the OS. Useful in this script to
# find the full path for 'hive' or if 'hive' is even install on the path.
def whereis(program):
	for path in os.environ.get('PATH', '').split(':'):
		if os.path.exists(os.path.join(path, program)) and not os.path.isdir(os.path.join(path, program)):
			return os.path.join(path, program)
	return None

# Validate that the date is in the YYYY-MM-DD format.
def is_valid_date(date):
	try:
	  date = date.split(" ")[0]
	  time.strptime(date, '%Y-%m-%d')
	  is_in_valid_format= True
	except ValueError:
	  is_in_valid_format = False
	return is_in_valid_format
	
# Validate that the date has a valid time in the format HH:MM:SS.
def has_valid_time(date):
	try:
	  date = date.split(" ")[1]
	  time.strptime(date, '%H:%M:%S')
	except ValueError:
	  return False
	except:
	  return False
	else:
	  return True

#This function turns a List into a Comma seperated string list.
def listToString(listObject):
	result = ""
	counter = 1
	for obj in listObject:
		result += obj
		if counter != len(listObject):
			result += ","
		counter += 1
	return result