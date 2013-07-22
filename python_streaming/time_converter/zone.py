#!/usr/bin/env python
# encoding: utf-8
"""
zone.py

"""
import re
import types
from datetime import datetime,tzinfo,timedelta

#Offset Regex Examples:
'''
Must Specify -/+
"-0400" - 4 hours behind UTC
"+0100" - 1 hour ahead of UTC
"+0230" - 2 hours, 30 minutes ahead of UTC
"-0000" or "+0000" - UTC
"00:00" - supports delimiter
'''
_offset_regex = re.compile(r'([+-]{1})([01][0-9]|2[0-3]):?([0-5][0-9])')

'''Allows Creation of Generic TimeZones'''
class Zone(tzinfo):
    def __init__(self,offset,isdst,name,offset_min=0,offset_sec=0):
        if type(offset) == types.StringType:
            mt =  _offset_regex.match(offset)
            if mt:
                offset = int(mt.groups()[0]+mt.groups()[1])
                offset_min = int(mt.groups()[0]+mt.groups()[2])
        self.offset = offset
        self.offset_min = offset_min
        self.isdst = isdst
        self.name = name
    def utcoffset(self, dt):
        return timedelta(hours=self.offset,minutes=self.offset_min) + self.dst(dt)
    def dst(self, dt):
        return timedelta(hours=1) if self.isdst else timedelta(0)
    def tzname(self,dt):
        return self.name


# helper function, which takes the parts
# of the matched offset regex and makes a delta

def _make_offset_delta(tup):
    delta = DateTimeDelta(0, int(tup[1]), int(tup[2]))
    if tup[0] == '-': delta =  -1 * delta
    return delta

def _match_offset(offset):
    try:
        mt = _offset_regex.match(offset)
        if mt:
            return True
        else:
            return False
    except:
        return False

#Test
if (__name__ == '__main__') :
    # Assume t is Local Time in PST
    t = datetime.strptime('2011-12-25 00:00:01','%Y-%m-%d %H:%M:%S')
    t_offset = '-07:00' #PDT
    # To convert to UTC we need to specify the Zone(offset) part of OFFSET
    LOCAL = Zone(t_offset,False,'Local')
    UTC = Zone(0,False,'UTC')
    # Original value is in UTC
    t = t.replace(tzinfo=UTC)
    print
    print "Zone:"
    print "--UTC:"
    GMT = Zone(0,False,'GMT')
    print "Time Now: " + datetime.now(GMT).strftime('%Y-%m-%d %H:%M:%S %z') + " DST(" + str(datetime.now(GMT).dst()) + ")" #('%m/%d/%Y %H:%M:%S %Z')
    #t = t.replace(tzinfo=GMT)
    print "X-Mas (Local): " + str(t)
    print "X-Mas (UTC): " + str(t.astimezone(GMT))
    print "--"
    print "--EST:"
    EST = Zone(-5,False,'EST')
    print "Time Now: " + datetime.now(EST).strftime('%Y-%m-%d %H:%M:%S %z') + " DST(" + str(datetime.now(EST).dst()) + ")" #('%m/%d/%Y %H:%M:%S %Z')
    print "X-Mas: " + str(t.astimezone(EST)) + " DST(" + str(t.astimezone(EST).dst()) + ")"
    print "--"
    print "--PST:"
    PST = Zone('-0800',False,'PST')
    print "Time Now: " + datetime.now(PST).strftime('%Y-%m-%d %H:%M:%S %z') + " DST(" + str(datetime.now(PST).dst()) + ")" #('%m/%d/%Y %H:%M:%S %Z')
    print "X-Mas: " + str(t.astimezone(PST)) + " DST(" + str(t.astimezone(PST).dst()) + ")"
    print "--"
    print "--OFFSET Local:"
    print "Time Now: " + datetime.now(LOCAL).strftime('%Y-%m-%d %H:%M:%S %z') + " DST(" + str(datetime.now(LOCAL).dst()) + ")" #('%m/%d/%Y %H:%M:%S %Z')
    print "X-Mas: " + str(t.astimezone(LOCAL)) + " DST(" + str(t.astimezone(LOCAL).dst()) + ")"
    print "--"
    print 'Done !'
    print
    if type('-0500') == types.StringType:
        mt =  _offset_regex.match('-0500')
        if mt:
            print str(mt.groups())
            print int(mt.groups()[0]+mt.groups()[1]) * 1
            print int(mt.groups()[0]+mt.groups()[2]) * 1
