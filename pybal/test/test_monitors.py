# -*- coding: utf-8 -*-
"""
  PyBal unit tests
  ~~~~~~~~~~~~~~~~

  This module contains tests for `pybal.monitors`.

"""
import unittest

import pybal.util
from pybal.monitors.idleconnection import IdleConnectionMonitoringProtocol

from .fixtures import PyBalTestCase


class IdleConnectionMonitoringProtocolTestCase(PyBalTestCase):
    """Test case for `pybal.monitors.IdleConnectionMonitoringProtocol`."""

    def setUp(self):
        self.config = pybal.util.ConfigDict()
        self.monitor = IdleConnectionMonitoringProtocol(
            None, None, self.config)

    def testInit(self):
        """Test `IdleConnectionMonitoringProtocol.__init__`."""
        monitor = IdleConnectionMonitoringProtocol(None, None, self.config)
        self.assertEquals(
            monitor.maxDelay, IdleConnectionMonitoringProtocol.MAX_DELAY)
        self.assertEquals(
            monitor.toCleanReconnect,
            IdleConnectionMonitoringProtocol.TIMEOUT_CLEAN_RECONNECT
        )
        self.config['idleconnection.max-delay'] = '123'
        self.config['idleconnection.timeout-clean-reconnect'] = '456'
        monitor = IdleConnectionMonitoringProtocol(None, None, self.config)
        self.assertEquals(monitor.maxDelay, 123)
        self.assertEquals(monitor.toCleanReconnect, 456)

    def testRun(self):
        """Test `IdleConnectionMonitoringProtocol.run`."""
        pass
