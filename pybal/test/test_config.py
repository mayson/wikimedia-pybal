# -*- coding: utf-8 -*-
"""
  PyBal unit tests
  ~~~~~~~~~~~~~~~~

  This module contains tests for `pybal.config`.

"""
import unittest
import tempfile
import os

import pybal
import pybal.config

from .fixtures import PyBalTestCase


class DummyConfigurationObserver(pybal.config.ConfigurationObserver):
    urlScheme = 'dummy://'

    def __init__(self, *args, **kwargs):
        pass


class ConfigurationObserverTestCase(PyBalTestCase):
    """Test case for `pybal.config.ConfigurationObserver`."""

    def testFromUrl(self):
        """Test `ConfigurationObserver.fromUrl`."""
        self.assertIsInstance(
            pybal.config.ConfigurationObserver.fromUrl(None, 'dummy://'),
            DummyConfigurationObserver
        )
