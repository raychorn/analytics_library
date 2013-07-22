#!/usr/bin/env python
import urllib2, simplejson
from pprint import pprint

def parse_json(definition, json):
  result.update(definition) 
  for k in definition.keys():
    try:
      if not isinstance(json[k], (basestring,float,int)):
        #result[k] = "NULL"
        print "result before recurse: " + str(result)
        result_ext = (parse_json(result,json[k]))
        print "result of result_ext: " + str(result_ext)
        print "result after recurse: " + str(result)
        result.update(result_ext)
        print "result.uptdate      : " + str(result)
        #print k + ":" + "PARENT"
      else:
        result[k] = str(json[k])
        #print k + ":" + str(json[k])
    except:
      #result[k] = "NULL"
      #print k + ":" + "MISSING"
      pass
  return result

#line = "2010-06-09 22:05:42	10.100.162.40	{\"d\":[ 3, \"2010-06-09 06:55:38\", \"SPRINT\", \"2.50.17.0\", \"89fb082a-f6d3-4808-8d18-ffda7d5dada1\", \"ApplicationInfo\", { \"15\": 1, \"23\": 1, \"13\": {}, \"28\": 0, \"34\": \"Unknown\" } ]}"
line = "2010-06-09 22:05:42	10.100.162.40	{\"d\":[ 3, \"2010-06-09 06:56:33\", \"SPRINT\", \"2.50.17.0\", \"89fb082a-f6d3-4808-8d18-ffda7d5dada1\", \"DeviceInsertion\", { \"DeviceType\": \"4GDevice\", \"Device\": { \"5\": 1, \"40\": \"f4-63-49-01-0b-bc\", \"6\": \"05.02.628050015\", \"7\": \"6600\", \"4\": \"bece3301\", \"41\": \"5.2.116.09000\", \"46\": \"2-13-2010\" }, \"24\": 24, \"25\": 0, \"26\": \"2010-06-05 13:59:44\", \"27\": \"2010-06-09 14:00:47\" } ]}"

fields = line.split("\t",5)
json = simplejson.loads(fields[2])
event = json['d'][5]

ext_json = simplejson.loads(simplejson.dumps(json['d'][6]))
pprint(ext_json)

definition = {}
f = open('DeviceInsertion','r')
for line in f.xreadlines():
  definition[str(line.strip())] = 'NULL'
f.close()

result = {}
result = parse_json(definition,ext_json)

map1 = {}
map1['a'] = 'A'
map1['c'] = 'C'
print "Map1       : " + str(map1)
map2 = {}
map2['b'] = 'B'
print "Map2       : " + str(map2)
map1.update(map2)
print "Map1 + Map2: " + str(map1)

print "Output     : " + str(result)

print "DeviceType : " + str(ext_json['DeviceType'])
print "c_24       : " + str(ext_json['24'])
print "event: " + event
