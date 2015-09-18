# -*- coding: utf-8 -*-
"""
  PyBal unit tests
  ~~~~~~~~~~~~~~~~

  This module contains tests for `pybal.pybal`.

"""
import sys
import mock
from .fixtures import PyBalTestCase
from pybal.pybal import parseCommandLine


class TestBaseUtils(PyBalTestCase):

    def test_parseCommandLine(self):
        """Test case for `pybal.pybal.parseCommandLine`"""
        testargs = ['pybal', '--dryrun', '--debug']
        config = {}
        with mock.patch.object(sys, 'argv', testargs):
            parseCommandLine(config)
            self.assertEquals(config, {'debug': True, 'dryrun': True})
        testargs.append('--badarg')
        # Bad argument raises an exception
        with mock.patch.object(sys, 'argv', testargs):
            self.assertRaises(Exception, parseCommandLine, config)
        # Asking for the help exits with 0 code
        testargs = ['pybal', '--help']
        with mock.patch.object(sys, 'argv', testargs):
            with self.assertRaises(SystemExit) as exc:
                parseCommandLine(config)
                self.assertEquals(exc.exception.code, 0)
