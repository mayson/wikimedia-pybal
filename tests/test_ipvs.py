# -*- coding: utf-8 -*-
"""
  PyBal unit tests
  ~~~~~~~~~~~~~~~~

  This module contains tests for `pybal.ipvs`.

"""
import unittest

import pybal.ipvs


class ServerStub(object):
    """Test stub for `pybal.Server`."""
    def __init__(self, host, ip=None, weight=None):
        self.host = host
        self.ip = ip
        self.weight = weight


class IPVSManagerTestCase(unittest.TestCase):
    """Test case for `pybal.ipvs.IPVSManager`."""

    def testSubCommandService(self):
        """Test `IPVSManager.subCommandService`."""
        # Maps tuples of (protocol, address, port) to string of ipvsadm
        # args declaring an equivalent virtual network service.
        services = {
            ('tcp', '2620::123', 443): '-t [2620::123]:443',
            ('udp', '208.0.0.1', 123): '-u 208.0.0.1:123',
        }
        for service, expected_subcommand in services.items():
            subcommand = pybal.ipvs.IPVSManager.subCommandService(service)
            self.assertEquals(subcommand, expected_subcommand)

    def testSubCommandServer(self):
        """Test `IPVSManager.subCommandServer`."""
        servers = {
            ServerStub('localhost', None): '-r localhost',
            ServerStub('localhost', '127.0.0.1'): '-r 127.0.0.1',
        }
        for server, expected_subcommand in servers.items():
            subcommand = pybal.ipvs.IPVSManager.subCommandServer(server)
            self.assertEquals(subcommand, expected_subcommand)

    def testCommandClearServiceTable(self):
        """Test `IPVSManager.commandClearServiceTable`."""
        subcommand = pybal.ipvs.IPVSManager.commandClearServiceTable()
        self.assertEquals(subcommand, '-C')

    def testCommandRemoveService(self):
        """Test `IPVSManager.commandRemoveService`."""
        services = {
            ('tcp', '2620::123', 443): '-D -t [2620::123]:443',
            ('udp', '208.0.0.1', 123): '-D -u 208.0.0.1:123',
        }
        for service, expected_subcommand in services.items():
            subcommand = pybal.ipvs.IPVSManager.commandRemoveService(service)
            self.assertEquals(subcommand, expected_subcommand)

    def testCommandAddService(self):
        """Test `IPVSManager.commandAddService`."""
        services = {
            ('tcp', '2620::123', 443): '-A -t [2620::123]:443',
            ('udp', '208.0.0.1', 123, 'rr'): '-A -u 208.0.0.1:123 -s rr',
        }
        for service, expected_subcommand in services.items():
            subcommand = pybal.ipvs.IPVSManager.commandAddService(service)
            self.assertEquals(subcommand, expected_subcommand)

    def testCommandRemoveServer(self):
        """Test `IPVSManager.commandRemoveServer`."""
        service = ('tcp', '2620::123', 443)
        server = ServerStub('localhost', None)
        subcommand = pybal.ipvs.IPVSManager.commandRemoveServer(
            service, server)
        self.assertEquals(subcommand, '-d -t [2620::123]:443 -r localhost')

    def testCommandAddServer(self):
        """Test `IPVSManager.commandAddServer`."""
        service = ('tcp', '2620::123', 443)

        server = ServerStub('localhost', None)
        subcommand = pybal.ipvs.IPVSManager.commandAddServer(service, server)
        self.assertEquals(subcommand, '-a -t [2620::123]:443 -r localhost')

        server.weight = 25
        subcommand = pybal.ipvs.IPVSManager.commandAddServer(service, server)
        self.assertEquals(
            subcommand, '-a -t [2620::123]:443 -r localhost -w 25')

    def testCommandEditServer(self):
        """Test `IPVSManager.commandEditServer`."""
        service = ('tcp', '2620::123', 443)

        server = ServerStub('localhost', None)
        subcommand = pybal.ipvs.IPVSManager.commandEditServer(service, server)
        self.assertEquals(subcommand, '-e -t [2620::123]:443 -r localhost')

        server.weight = 25
        subcommand = pybal.ipvs.IPVSManager.commandEditServer(service, server)
        self.assertEquals(
            subcommand, '-e -t [2620::123]:443 -r localhost -w 25')
