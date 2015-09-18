# -*- coding: utf-8 -*-
"""
  PyBal unit tests
  ~~~~~~~~~~~~~~~~

  This module contains tests for `pybal.util`.

"""
import unittest
import tempfile
import os

import pybal
import pybal.util

from .fixtures import PyBalTestCase


class MiscUtilTestCase(PyBalTestCase):
    """Test case for misc. methods in `pybal.util`."""

    def testGetSubclasses(self):
        """Test case for `pybal.util.get_subclasses`."""

        class DummyParent(object):
            pass

        class DummyChild(DummyParent):
            pass

        class DummyGrandChild(DummyChild):
            pass

        subclasses = pybal.util.get_subclasses(DummyParent)
        subclasses.sort(key=lambda cls: cls.__name__)
        self.assertEqual(subclasses, [DummyChild, DummyGrandChild])


class ConfigDictTestCase(PyBalTestCase):
    """Test case for `pybal.util.ConfigDict`."""

    def setUp(self):
        super(ConfigDictTestCase, self).setUp()
        self.config.update({
            'int': '3',
            'truthy': 'true',
            'falsy': 'false',
            'float': '3.14',
        })

    def testGetInt(self):
        """Test `ConfigDict.getint()`."""
        self.assertEqual(self.config.getint('int'), 3)
        self.assertEqual(self.config.getint('missing', 4), 4)
        with self.assertRaises(KeyError):
            self.config.getint('missing')
        with self.assertRaises(ValueError):
            self.config.getint('truthy')

    def testGetFloat(self):
        """Test `ConfigDict.getfloat()`."""
        self.assertEqual(self.config.getfloat('float'), 3.14)
        self.assertEqual(self.config.getfloat('missing', True), True)
        with self.assertRaises(KeyError):
            self.config.getfloat('missing')
        with self.assertRaises(ValueError):
            self.config.getfloat('falsy')

    def testGetBoolean(self):
        """Test `ConfigDict.getboolean()`."""
        self.assertEqual(self.config.getboolean('truthy'), True)
        self.assertEqual(self.config.getboolean('falsy'), False)
        self.assertEqual(self.config.getboolean('missing', True), True)
        with self.assertRaises(KeyError):
            self.config.getboolean('missing')
        with self.assertRaises(ValueError):
            self.config.getboolean('float')
