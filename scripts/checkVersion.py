#!/usr/bin/env python
# encoding: utf-8
"""
checkVersion.py

"""
import os
import sys
import urllib
import optparse
import simplejson

__author__  = "Luis Ramos"
__email__   = "lramos@smithmicro.com"
__date__    = "2010-11-16"
__version__ = 0.2

#Get Version Strings
checkVersion_ver = __version__
analytics_ver = "1.1"
analytics_lib_ver = "1.1-r14321"
hadoop_ver = "0.20.2-append"
hive_ver = "0.6"
derby_ver = "10.5.1.1"
hbase_ver = "trunk"

#Analytics Library Directory
analytics_lib = str(os.getenv("ANALYTICS_LIB"))
#Hadoop Home Directory
hadoop_home = str(os.getenv("HADOOP_HOME"))
#Hive Home Directory
hive_home = str(os.getenv("HIVE_HOME"))
#Derby Home Directory
derby_home = str(os.getenv("DERBY_HOME"))

def get_version_json(requests):
	result = []
	versionInfo = '{"versions": { \
		"checkVersion":{"checkVersion":{"version":"%(checkVersion)s"}}, \
		"analytics":{"analytics":{"version":"%(analytics)s"}}, \
		"analytics_lib":{"analytics":{"version":"%(analytics_lib)s"}}, \
		"hadoop":{"hadoop":{"version":"%(hadoop)s"}}, \
		"hive":{"hive":{"version":"%(hive)s"}}, \
		"derby":{"derby":{"version":"%(derby)s"}}, \
		"hbase":{"hbase":{"version":"%(hbase)s"}}  }}' % \
		{
		 'checkVersion':checkVersion_ver,
		 'analytics':analytics_ver,
		 'analytics_lib':analytics_lib_ver,
		 'hadoop':hadoop_ver,
		 'hive':hive_ver,
		 'derby':derby_ver,
		 'hbase':hbase_ver
		}
	json = simplejson.loads(versionInfo)
	if requests.all:
		result.append(json)
	else:
		if requests.analytics:
			result.append(json['versions']['analytics'])
		if requests.hadoop:
			result.append(json['versions']['hadoop'])
		if requests.hive:
			result.append(json['versions']['hive'])
		if requests.derby:
			result.append(json['versions']['derby'])
		if requests.hbase:
			result.append(json['versions']['hbase'])
	return result

def get_version_str(requests):
	result = []
	versionInfo = {"checkVersion":str(checkVersion_ver),"analytics":str(analytics_ver),"analytics_lib":str(analytics_lib_ver),"hadoop":str(hadoop_ver),"hive":str(hive_ver),"derby":str(derby_ver),"hbase":str(hbase_ver)}
	if requests.all:
		result.append(str(versionInfo))
	else:
		if requests.analytics:
			result.append("analytics:" + str(versionInfo['analytics']))
		if requests.analytics:
			result.append("analytics_lib:" + str(versionInfo['analytics_lib']))
		if requests.hadoop:
			result.append("hadoop:" + str(versionInfo['hadoop']))
		if requests.hive:
			result.append("hive:" + str(versionInfo['hive']))
		if requests.derby:
			result.append("derby:" + str(versionInfo['derby']))
		if requests.hbase:
			result.append("hbase:" + str(versionInfo['hbase']))
	return result
	return

def main(argv=None):
	""" Main Application Handler """
	usage = "usage: %%prog [options]"
	version_string = "%%prog %s" % __version__
	description = "checkVersion.py - Returns the version of each of the components for SMSI Analytics"
	
	# Create our parser and setup our command line options
	parser = optparse.OptionParser(usage=usage, version=version_string, description=description)
	
	parser.add_option("-j", "--json", action="store_true", dest="json", default=False, help="JSON Format")
	parser.add_option("-s", "--string", action="store_true", dest="string", default=True, help="String Format")
	parser.add_option("-a", "--all", action="store_true", dest="all", default=False, help="Include All Versions")
	parser.add_option("--analytics", action="store_true", dest="analytics", default=False, help="Include Analytics Version")
	parser.add_option("--hadoop", action="store_true", dest="hadoop", default=False, help="Include Hadoop Version")
	parser.add_option("--hive", action="store_true", dest="hive", default=False, help="Include Hive Version")
	parser.add_option("--derby", action="store_true", dest="derby", default=False, help="Include Derby Version")
	parser.add_option("--hbase", action="store_true", dest="hbase", default=False, help="Include HBase Version")
	parser.add_option("-c", "--changes", action="store_true", dest="changes", default=False, help="Include Revision/Changes Information")
	
	# Parse our options and arguments   
	options, args = parser.parse_args()
	
	# Start Checking Parser Options:
	# Check if Changes should be included
	print "Options:"
	print options, args
	if options.changes:
		print "Include Changes NOT IMPLEMENTED"
		
	if options.json:
		options.string = False
		print get_version_json(options)
	
	if options.string:
		options.json = False
		print get_version_str(options)
		

	
if __name__ == "__main__":
	sys.exit(main())