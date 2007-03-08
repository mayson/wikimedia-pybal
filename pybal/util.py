"""
util.py - PyBal utility classes
Copyright (C) 2006 by Mark Bergsma <mark@nedworks.org>

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
