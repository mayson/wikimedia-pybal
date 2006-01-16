"""
monitor.py
Copyright (C) 2006 by Mark Bergsma <mark@nedworks.org>

Monitor class implementations for PyBal

$Id$
"""

from twisted.internet import reactor, protocol
from twisted.web import client
from twisted.python.runtime import seconds

import pybal

class MonitoringProtocol(object):
    """
    Base class for all monitoring protocols. Declares a few obligatory
    abstract methods, and some commonly useful functions  
    """
    
    def __init__(self, coordinator, server):
        """Constructor"""
        
        self.coordinator = coordinator
        self.server = server
        self.up = None    # None, False (Down) or True (Up)
    
        self.firstCheck = True
    
    def run(self):
        """Start the monitoring"""
        
        raise NotImplementedError
    
    def stop(self):
        """Stop the monitoring; cancel any running or upcoming checks"""
        
        raise NotImplementedError

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

class ProxyFetchMonitoringProtocol(MonitoringProtocol):
    """
    Monitor that checks server uptime by repeatedly fetching a certain URL
    """    
    
    INTV_CHECK = 10
    
    TIMEOUT_GET = 5
    
    __name__ = 'ProxyFetch'
    
    from twisted.internet import defer, error
    from twisted.web import error as weberror
    catchList = ( defer.TimeoutError, weberror.Error, error.ConnectError )
    
    def __init__(self, coordinator, server):
        """Constructor"""
        
        # Call ancestor constructor
        super(ProxyFetchMonitoringProtocol, self).__init__(coordinator, server)
                
        self.intvCheck = self.INTV_CHECK
        self.toGET = self.TIMEOUT_GET
        
        self.checkCall = None
        
        self.checkStartTime = None
        
        self.URL = [ 'http://en.wikipedia.org/wiki/Main_Page',
                     'http://en.wikipedia.org/wiki/Special:Version' ]
    
    def run(self):
        """Start the monitoring"""
        
        self.checkCall = reactor.callLater(self.intvCheck, self.check)
    
    def stop(self):
        """Stop all running and/or upcoming checks"""
        
        if self.checkCall and self.checkCall.active():
            self.checkCall.cancel()
            
        # TODO: cancel a getPage as well        
        
    def check(self):
        """Periodically called method that does a single uptime check."""
        
        import random
        url = random.choice(self.URL)
        
        self.checkStartTime = seconds()
        self.getProxyPage(url, method='HEAD', host=self.server.host, port=self.server.port,
                            timeout=self.toGET, followRedirect=False
            ).addCallbacks(self._fetchSuccessful, self._fetchFailed
            ).addBoth(self._checkFinished)
    
    def _fetchSuccessful(self, result):
        """Called when getProxyPage is finished successfully."""        
        
        print self.server.host + ': Fetch successful,', seconds() - self.checkStartTime, 's'
        self._resultUp()
        
        return result
    
    def _fetchFailed(self, failure):
        """Called when getProxyPage finished with a failure."""        
                
        print self.server.host + ': Fetch failed,', seconds() - self.checkStartTime, 's'
    
        self._resultDown(failure.getErrorMessage())
        
        failure.trap(*self.catchList)

    def _checkFinished(self, result):
        """
        Called when getProxyPage finished with either success or failure,
        to do after-check cleanups.
        """
        
        self.checkStartTime = None
        
        # Schedule the next check
        reactor.callLater(self.intvCheck, self.check)
        
        return result

    def getProxyPage(url, contextFactory=None, host=None, port=None, *args, **kwargs):
        """Download a web page as a string. (modified from twisted.web.client.getPage)
    
        Download a page. Return a deferred, which will callback with a
        page (as a string) or errback with a description of the error.
    
        See HTTPClientFactory to see what extra args can be passed.
        """

        factory = client.HTTPClientFactory(url, *args, **kwargs)
        
        host = host or factory.host
        port = port or factory.port

        if factory.scheme == 'https':
            from twisted.internet import ssl
            if contextFactory is None:
                contextFactory = ssl.ClientContextFactory()
            reactor.connectSSL(host, port, factory, contextFactory)
        else:
            reactor.connectTCP(host, port, factory)
        return factory.deferred
    getProxyPage = staticmethod(getProxyPage)

class IdleConnectionMonitoringProtocol(MonitoringProtocol, protocol.ReconnectingClientFactory):
    """
    Monitor that checks uptime by keeping an idle TCP connection open to the
    server. When the connection is closed in an unclean way, or when the connection
    is closed cleanly but a fast reconnect fails, the monitoring state is set to down.
    """
    
    protocol = protocol.Protocol

    TIMEOUT_CLEAN_RECONNECT = 3

    __name__ = 'IdleConnection'
    
    def __init__(self, coordinator, server):
        """Constructor"""
        
        # Call ancestor constructor        
        super(IdleConnectionMonitoringProtocol, self).__init__(coordinator, server)
        
        self.toCleanReconnect = self.TIMEOUT_CLEAN_RECONNECT
        
    def run(self):
        """Start the monitoring"""
        
        self._connect()
    
    def stop(self):
        """Stop all running and/or upcoming checks"""
        
        raise NotImplementedError
    
    def clientConnectionFailed(self, connector, reason):
        """Called if the connection attempt failed"""
        
        # Immediately set status to down
        self._resultDown(reason.getErrorMessage())
        
        # Slowly reconnect
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        """Called if the connection was previously established, but lost at some point."""
        
        from twisted.internet import error
        if reason.check(error.ConnectionDone):
            # Connection lost in a clean way. May be idle timeout - try a fast reconnect
            self._connect(timeout=self.toCleanReconnect)
        else:
            # Connection lost in a non clean way. Immediately set status to down
            self._resultDown(reason.getErrorMessage())            

            # Slowly reconnect
            protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    
    def clientConnectionMade(self):
        """Called by buildProtocol, to notify that the connection has been established."""
        
        # Set status to up
        self._resultUp()
       
        # Reset reconnection delay
        self.resetDelay()
    
    def buildProtocol(self, addr):
        """
        Called to build a new Protocol instance. Implies that the TCP connection
        has been established successfully.
        """
        
        self.clientConnectionMade()
        
        # Let the ancestor method do the real work
        return super(IdleConnectionMonitoringProtocol, self).buildProtocol(addr)
    
    def _connect(self, *args, **kwargs):
        """Starts a TCP connection attempt"""
        
        reactor.connectTCP(self.server.host, self.server.port, self, *args, **kwargs)