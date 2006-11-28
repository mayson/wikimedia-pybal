"""
monitor.py
Copyright (C) 2006 by Mark Bergsma <mark@nedworks.org>

Monitor class implementations for PyBal

$Id$
"""

class MonitoringProtocol(object):
    """
    Base class for all monitoring protocols. Declares a few obligatory
    abstract methods, and some commonly useful functions  
    """
    
    def __init__(self, coordinator, server, configuration={}):
        """Constructor"""
        
        self.coordinator = coordinator
        self.server = server
        self.configuration = configuration
        self.up = None    # None, False (Down) or True (Up)
    
        self.active = False
        self.firstCheck = True
    
    def run(self):
        """Start the monitoring"""

        assert(self.active == False)
       
        self.active = True
    
    def stop(self):
        """Stop the monitoring; cancel any running or upcoming checks"""
        
        self.active = False

    def name(self):
        """Returns a printable name for this monitor"""
        
        return self.__name__

    def _resultUp(self):
        """
        Sets own monitoring state to Up and notifies the coordinator
        if this implies a state change
        """
        
        if self.up == False or self.firstCheck:
            self.up = True
            self.firstCheck = False
            if self.coordinator:
                self.coordinator.resultUp(self)

    def _resultDown(self, reason=None):
        """
        Sets own monitoring state to Down and notifies the coordinator
        if this implies a state change
        """        
        
        if self.up == True or self.firstCheck:
            self.up = False
            self.firstCheck = False
            if self.coordinator:
                self.coordinator.resultDown(self, reason)
    
    def _getConfigStringList(self, optionname):
        """
        Takes a (string) value, eval()s it and checks whether it
        consists of either a single string, or a single list of
        strings
        """
        
        val = eval(self.configuration[self.__name__.lower() + '.' + optionname])
        if type(val) == str:
            return val
        elif type(val) == list and reduce(lambda x, y: type(x) == str and y, val):
            # Checked that each list member is a string
            return val
        else:
            raise ValueError, "Value '%s' is not a string or stringlist" % val
    