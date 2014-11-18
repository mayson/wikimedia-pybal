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


class LogFileTestCase(PyBalTestCase):
    """Test case for `pybal.util.LogFile`."""

    TIMESTAMP_REGEXP = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+'

    def setUp(self):
        super(LogFileTestCase, self).setUp()
        file_handle, self.path = tempfile.mkstemp('.pybal.test.log')
        os.close(file_handle)
        self.log_file = pybal.util.LogFile(self.path)

    def tearDown(self):
        self.log_file.file.close()
        os.unlink(self.path)

    def testWrite(self):
        """Test `LogFile.write`."""
        log_lines = ('line 1\n', 'line 2\n', 'line 3\n')
        self.log_file.write(''.join(log_lines))
        with open(self.path, 'r') as f:
            file_lines = f.readlines()
        self.assertEqual(len(file_lines), 3)
        for log_line, file_line in zip(log_lines, file_lines):
            self.assertIn(log_line, file_line)
            self.assertRegexpMatches(file_line, self.TIMESTAMP_REGEXP)

    def testReopen(self):
        """Test `LogFile.reopen`."""
        os.unlink(self.path)
        self.log_file.reopen()
        self.log_file.write('test')
        self.assertTrue(os.path.exists(self.path))
        with open(self.path, 'r') as f:
            self.assertIn('test', f.read())


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
