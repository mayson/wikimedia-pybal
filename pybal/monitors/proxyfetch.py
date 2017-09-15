"""
proxyfetch.py
Copyright (C) 2006 by Mark Bergsma <mark@nedworks.org>

Monitor class implementations for PyBal
"""

from pybal import monitor, util

from twisted.internet import reactor, defer
from twisted.web import client
from twisted.python.runtime import seconds
import logging, random

log = util.log

# taken from twisted/twisted/internet/_sslverify.py
try:
    from OpenSSL.SSL import SSL_CB_HANDSHAKE_DONE, SSL_CB_HANDSHAKE_START
except ImportError:
    SSL_CB_HANDSHAKE_START = 0x10
    SSL_CB_HANDSHAKE_DONE = 0x20
from twisted.internet._sslverify import (ClientTLSOptions,
                                         _maybeSetHostNameIndication,
                                         verifyHostname,
                                         VerificationError)
from OpenSSL.SSL import OP_ALL
from twisted.internet.ssl import ClientContextFactory

class ScrapyClientTLSOptions(ClientTLSOptions):
    """
    SSL Client connection creator ignoring certificate verification errors
    (for genuinely invalid certificates or bugs in verification code).
    Same as Twisted's private _sslverify.ClientTLSOptions,
    except that VerificationError and ValueError exceptions are caught,
    so that the connection is not closed, only logging warnings.
    """

    def _identityVerifyingInfoCallback(self, connection, where, ret):
        if where & SSL_CB_HANDSHAKE_START:
            _maybeSetHostNameIndication(connection, self._hostnameBytes)
        elif where & SSL_CB_HANDSHAKE_DONE:
            try:
                verifyHostname(connection, self._hostnameASCII)
            except VerificationError as e:
                log.warn(
                    'Remote certificate is not valid for hostname "{}"; {}'.format(
                        self._hostnameASCII, e))

            except ValueError as e:
                log.warn(
                    'Ignoring error while verifying certificate '
                    'from host "{}" (exception: {})'.format(
                        self._hostnameASCII, repr(e)))

class SSLClientContextFactory(ClientContextFactory):

    def __init__(self, hostname=None):
        self.hostname = hostname

    def getContext(self):
        ctx = ClientContextFactory.getContext(self)
        # Enable all workarounds to SSL bugs as documented by
        # http://www.openssl.org/docs/ssl/SSL_CTX_set_options.html
        ctx.set_options(OP_ALL)
        if self.hostname:
            ScrapyClientTLSOptions(self.hostname, ctx)
        return ctx

class RedirHTTPPageGetter(client.HTTPPageGetter):
    """PageGetter that accepts redirects as valid responses"""

    def handleStatus_301(self):
        """If we get a redirect, that's ok"""
        # Twisted uses old-style classes, yuck
        return client.HTTPPageGetter.handleStatus_200(self)

    def handleStatus_200(self):
        """Fail on 200 (we're expecting a redirect here)"""
        return self.handleStatusDefault()


class RedirHTTPClientFactory(client.HTTPClientFactory):
    """HTTPClientFactory that accepts redirects as valid responses"""
    protocol = RedirHTTPPageGetter


class ProxyFetchMonitoringProtocol(monitor.MonitoringProtocol):
    """
    Monitor that checks server uptime by repeatedly fetching a certain URL
    """

    INTV_CHECK = 10

    TIMEOUT_GET = 5

    HTTP_STATUS = 200

    __name__ = 'ProxyFetch'

    from twisted.internet import error
    from twisted.web import error as weberror
    catchList = ( defer.TimeoutError, weberror.Error, error.ConnectError, error.DNSLookupError )

    def __init__(self, coordinator, server, configuration={}):
        """Constructor"""

        # Call ancestor constructor
        super(ProxyFetchMonitoringProtocol, self).__init__(coordinator, server, configuration)

        self.intvCheck = self._getConfigInt('interval', self.INTV_CHECK)
        self.toGET = self._getConfigInt('timeout', self.TIMEOUT_GET)
        self.expectedStatus = self._getConfigInt('http_status',
                                                 self.HTTP_STATUS)

        self.checkCall = None
        self.getPageDeferred = defer.Deferred()

        self.checkStartTime = None

        self.URL = self._getConfigStringList('url')

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

        self.getPageDeferred.cancel()

    def check(self):
        """Periodically called method that does a single uptime check."""

        if not self.active:
            log.warn("ProxyFetchMonitoringProtocol.check() called while active == False")
            return

        # FIXME: Use GET as a workaround for a Twisted bug with HEAD/Content-length
        # where it expects a body and throws a PartialDownload failure

        url = random.choice(self.URL)

        self.checkStartTime = seconds()
        self.getPageDeferred = self.getProxyPage(
            url,
            method='GET',
            host=self.server.ip,
            port=self.server.port,
            status=self.expectedStatus,
            timeout=self.toGET,
            followRedirect=False
        ).addCallbacks(
            self._fetchSuccessful,
            self._fetchFailed
        ).addBoth(self._checkFinished)

    def _fetchSuccessful(self, result):
        """Called when getProxyPage is finished successfully."""

        self.report('Fetch successful, %.3f s' % (seconds() - self.checkStartTime))
        self._resultUp()

        return result

    def _fetchFailed(self, failure):
        """Called when getProxyPage finished with a failure."""

        # Don't act as if the check failed if we cancelled it
        if failure.check(defer.CancelledError):
            return None

        self.report('Fetch failed, %.3f s' % (seconds() - self.checkStartTime),
                    level=logging.WARN)

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

    def getProxyPage(url, contextFactory=None, host=None, port=None,
                     status=None, *args, **kwargs):
        """Download a web page as a string. (modified from twisted.web.client.getPage)

        Download a page. Return a deferred, which will callback with a
        page (as a string) or errback with a description of the error.

        See HTTPClientFactory to see what extra args can be passed.
        """
        if status > 300 and status < 304:
            factory = RedirHTTPClientFactory(url, *args, **kwargs)
        else:
            factory = client.HTTPClientFactory(url, *args, **kwargs)

        host = host or factory.host
        # We should connect to different port taken from URI if specified
        port = factory.port or port

        if factory.scheme == 'https':
            if contextFactory is None:
                contextFactory = SSLClientContextFactory(factory.host)
            reactor.connectSSL(host, port, factory, contextFactory)
        else:
            reactor.connectTCP(host, port, factory)
        return factory.deferred
    getProxyPage = staticmethod(getProxyPage)
