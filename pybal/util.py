"""
util.py - PyBal utility classes
Copyright (C) 2006-2008 by Mark Bergsma <mark@nedworks.org>

LVS Squid balancer/monitor for managing the Wikimedia Squid servers using LVS
"""

import sys, datetime

class LogFile(object):
    def __init__(self, filename):
        self.filename = filename
        self.lineEnded = True
                
        self.file = file(filename, 'a')
    
    def write(self, s):
        """
        Write string to logfile with a timestamp
        """

        lines = s.splitlines(True)
        for line in lines:
            if self.lineEnded:
                timestamp = datetime.datetime.now()
                self.file.write(str(timestamp) + ' ')
            self.file.write(line)
            self.lineEnded = line.endswith('\n')

        self.file.flush()
    
    def reopen(self):
        """
        Close the logfile and reopen it. Useful for log rotation.
        """
        
        self.file.close()
        self.file = file(self.filename, 'a')
        self.lineEnded = True

class ConfigDict(dict):
    
    def getint(self, key, default=None):
        try:
            return int(self[key])
        except KeyError:
            if default is not None:
                return default
            else:
                raise
        # do not intercept ValueError
    
    def getboolean(self, key, default=None):
        try:
            value = self[key].strip().lower()
        except KeyError:
            if default is not None:
                return default
            else:
                raise
        else:
            if value in ('t', 'true', 'y', 'yes', 'on', '1'):
                return True
            elif value in ('f', 'false', 'n', 'no', 'off', '0'):
                return False
            else:
                raise ValueError
    
    def getfloat(self, key, default=None):
        try:
            return float(self[key])
        except KeyError:
            if default is not None:
                return default
            else:
                raise
        # do not intercept ValueError