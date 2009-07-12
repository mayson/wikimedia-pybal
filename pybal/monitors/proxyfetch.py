"""
proxyfetch.py
Copyright (C) 2006 by Mark Bergsma <mark@nedworks.org>

Monitor class implementations for PyBal

$Id: monitor.py 17191 2006-10-22 11:33:00Z mark $
"""

from pybal import monitor

from twisted.internet import reactor, protocol
from twisted.web import client
from twisted.python.runtime import seconds

class ProxyFetchMonitoringProtocol(monitor.MonitoringProtocol):
    """
    Monitor that checks server uptime by repeatedly fetching a certain URL
    """    
    
    INTV_CHECK = 10
    
    TIMEOUT_GET = 5
    
    __name__ = 'ProxyFetch'
    
    from twisted.internet import defer, error
    from twisted.web import error as weberror
    catchList = ( defer.TimeoutError, weberror.Error, error.ConnectError )
    
    def __init__(self, coordinator, server, configuration={}):
        """Constructor"""
        
        # Call ancestor constructor
        super(ProxyFetchMonitoringProtocol, self).__init__(coordinator, server, configuration)
                
        self.intvCheck = self._getConfigInt('interval', self.INTV_CHECK)
        self.toGET = self._getConfigInt('timeout', self.TIMEOUT_GET)
        
        self.checkCall = None
        
        self.checkStartTime = None
        
        self.URL = self._getConfigStringList('url')
        
        # Install cleanup handler
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
    
    def run(self):
        """Start the monitoring"""
        
        super(ProxyFetchMonitoringProtocol, self).run()
        
        if not self.checkCall or not self.checkCall.active():
            self.checkCall = reactor.callLater(self.intvCheck, self.check)
    
    def stop(self):
        """Stop all running and/or upcoming checks"""
        
        super(ProxyFetchMonitoringProtocol, self).stop()

        if self.checkCall and self.checkCall.active():
            self.checkCall.cancel()
            
        # TODO: cancel a getPage as well        
        
    def check(self):
        """Periodically called method that does a single uptime check."""
        
        # FIXME: Check if this monitoring instance is even still active
        
        # FIXME: Use GET as a workaround for a Twisted bug with HEAD/Content-length
        # where it expects a body and throws a PartialDownload failure
        
        import random
        url = random.choice(self.URL)
        
        self.checkStartTime = seconds()
        self.getProxyPage(url, method='GET', host=self.server.host, port=self.server.port,
                            timeout=self.toGET, followRedirect=False
            ).addCallbacks(self._fetchSuccessful, self._fetchFailed
            ).addBoth(self._checkFinished)
    
    def _fetchSuccessful(self, result):
        """Called when getProxyPage is finished successfully."""        
        
        self.report('Fetch successful, %.3f s' % (seconds() - self.checkStartTime))
        self._resultUp()
        
        return result
    
    def _fetchFailed(self, failure):
        """Called when getProxyPage finished with a failure."""        
                
        self.report('Fetch failed, %.3f s' % (seconds() - self.checkStartTime))
    
        self._resultDown(failure.getErrorMessage())
        
        failure.trap(*self.catchList)

    def _checkFinished(self, result):
        """
        Called when getProxyPage finished with either success or failure,
        to do after-check cleanups.
        """
        
        self.checkStartTime = None
        
        # Schedule the next check
        if self.active:
            self.checkCall = reactor.callLater(self.intvCheck, self.check)
        
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
