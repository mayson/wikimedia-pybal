# -*- coding: utf-8 -*-
"""
  PyBal unit tests
  ~~~~~~~~~~~~~~~~

  This module contains fixtures and helpers for PyBal's test suite.

"""
import unittest

import pybal.util
import twisted.test.proto_helpers
import twisted.trial.unittest


class ServerStub(object):
    """Test stub for `pybal.Server`."""
    def __init__(self, host, ip=None, weight=None, port=None):
        self.host = host
        self.ip = ip
        self.weight = weight
        self.port = port
        self.ip4_addresses = set()
        self.ip6_addresses = set()
        if ip is not None:
            (self.ip6_addresses if ':' in ip else self.ip4_addresses).add(ip)

    def __hash__(self):
        return hash((self.host, self.ip, self.weight, self.port))


class StubCoordinator(object):
    """Test stub for `pybal.pybal.Coordinator`."""

    def __init__(self):
        self.up = None
        self.reason = None

    def resultUp(self, monitor):
        self.up = True

    def resultDown(self, monitor, reason=None):
        self.up = False
        self.reason = reason


class PyBalTestCase(twisted.trial.unittest.TestCase):
    """Base class for PyBal test cases."""

    # Use the newer `TestCase.assertRaises` in Python 2.7's stdlib
    # rather than the one provided by twisted.trial.unittest.
    assertRaises = unittest.TestCase.assertRaises

    def setUp(self):
        self.coordinator = StubCoordinator()
        self.config = pybal.util.ConfigDict()
        self.server = ServerStub(host='localhost', ip='127.0.0.1', port=80)
        self.reactor = twisted.test.proto_helpers.MemoryReactor()
