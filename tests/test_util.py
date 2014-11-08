# -*- coding: utf-8 -*-
"""
  PyBal unit tests
  ~~~~~~~~~~~~~~~~

  This module contains tests for `pybal.util`.

"""
import unittest

import pybal
import pybal.util


class ConfigDictTestCase(unittest.TestCase):
    """Test case for `pybal.util.ConfigDict`."""

    def setUp(self):
        self.config = pybal.util.ConfigDict({
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
