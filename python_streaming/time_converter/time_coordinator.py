#!/usr/bin/env python
# encoding: utf-8
"""
time_coordinator.py

"""
import re
import types
import zone
from datetime import datetime
"""This class uses a date/time string object"""
class TimeCoordinator():
    '''
    Accepts a date/time stamp of type string, eventually should support
    many different formats.
    Then transforms to other formats or timezones.
    '''
    '''Accepts date format: "2011-10-05 09:02:55-07:00"'''
    def __init__(self, date):
        self.date = date
        self.offset = 0
        self._parse_date()
     
    '''using offset to type Zone.'''
    def convert_date_to_local(self):
        result = self.date
        result = 'failed'
        try:
            date = datetime.strptime(self.date,'%Y-%m-%d %H:%M:%S')
            LOCAL = zone.Zone(self.offset,False,'Local')
            UTC = zone.Zone(0,False,'UTC')
            date = date.replace(tzinfo=UTC)
            result = str(date.astimezone(LOCAL))[:-6]
        except:
            pass
        return result
    
    '''TODO: to/from type Zone.'''
    def convert_date(self,from_zone,to_zone):
        return self.date 

    '''parse date and get offset information'''
    def _parse_date(self):
        #TODO: Check if Date Object, and other Formats
        date = self.date
        offset = self.offset
        if type(date) == types.StringType:
            offset = date[-6:]
            # if offset is of correct format
            # initiate variables in class
            if zone._match_offset(offset) is True:
                self.date = date[:-6]
                self.offset = offset

#Test
if (__name__ == '__main__') :
    print
    print "-- time_coordinator.py"
    TC = TimeCoordinator('2011-10-05 09:02:55-07:00')
    print "Date: " + str(TC.date)
    print "Offset: " + str(TC.offset)
    print zone._match_offset(TC.offset)
    print TC.convert_date_to_local()
    print "--"
    TC = TimeCoordinator('2011-10-12 22:30:54')
    print "Date: " + str(TC.date)
    print "Offset: " + str(TC.offset)
    print zone._match_offset(TC.offset)
    print TC.convert_date_to_local()
    print "--"
