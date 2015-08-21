# -*- coding: utf-8 -*-
"""
  PyBal config
  ~~~~~~~~~~~~

  This module implements handling of server configuration.

"""
from __future__ import absolute_import

import ast
import json
import logging
import os
import re

from twisted.internet import task
from twisted.python import log
from twisted.web import client


def get_subclasses(cls):
    """Return a list of all direct and indirect subclasses of a given class."""
    subclasses = []
    for subclass in cls.__subclasses__():
        subclasses.append(subclass)
        subclasses.extend(get_subclasses(subclass))
    return subclasses


class PyBalConfigurationError(Exception):
    """Raised when PyBal encounters a configuration it does not understand."""
    pass


class ConfigurationObserver(object):
    @classmethod
    def fromUrl(cls, coordinator, configUrl):
        """Construct an instance of the appropriate subclass for a URL."""
        for subclass in get_subclasses(cls):
            if configUrl.startswith(subclass.urlScheme):
                return subclass(coordinator, configUrl)
        raise PyBalConfigurationError('No handler for URL "%s"' % configUrl)


class FileConfigurationObserver(ConfigurationObserver):
    """ConfigurationObserver for local configuration files.

    Handles the 'file://' scheme.
    For example: 'file:///etc/pybal/pools/apache'.

    If the file name ends in '.json', treat it as a new-style configuration
    file, and expect the following format:

        {
          "pybal-test2002.codfw.wmnet": {
            "enabled": false,
            "weight": 10
          },
          "pybal-test2003.codfw.wmnet": {
            "enabled": true,
            "weight": 5
          }
        }

    If the file name does NOT end in '.json', treat it as an old-style (eval)
    configuration file, and expect the following format:

        { 'host': 'pybal-test2002.codfw.wmnet', 'weight':10, 'enabled': True }
        { 'host': 'pybal-test2003.codfw.wmnet', 'weight':10, 'enabled': True }

    """

    urlScheme = 'file://'

    def __init__(self, coordinator, configUrl, reloadIntervalSeconds=1):
        self.coordinator = coordinator
        self.configUrl = configUrl
        self.filePath = configUrl[len(self.urlScheme):]
        self.reloadIntervalSeconds = reloadIntervalSeconds
        self.lastFileStat = None
        self.lastConfig = None
        self.reloadTask = task.LoopingCall(self.reloadConfig)
        self.startObserving()

    def startObserving(self):
        """Start (or re-start) watching the configuration file for changes."""
        self.reloadTask \
            .start(self.reloadIntervalSeconds) \
            .addErrback(self.logError)

    def logError(self, failure):
        """Log an error and re-schedule the configuration file monitor."""
        failure.trap(Exception)
        log.err(failure)
        self.fileStat = None
        if not self.reloadTask.running:
            self.startObserving()

    def parseLegacyConfig(self, rawConfig):
        """Parse a legacy (eval) configuration file."""
        config = {}
        for line in rawConfig.split('\n'):
            try:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                server = ast.literal_eval(line)
                host = server.pop('host')
                config[host] = server
            except (KeyError, SyntaxError, TypeError, ValueError) as ex:
                # We catch exceptions here (rather than simply allow them to
                # bubble up to FileConfigurationObserver.logError) because we
                # want to try and parse as much of the file as we can.
                log.err(ex, 'Bad configuration line: %s' % line)
                continue
        return config

    def parseJsonConfig(self, rawConfig):
        """Parse a JSON pool configuration file."""
        return json.loads(rawConfig)

    def parseConfig(self, rawConfig):
        if self.configUrl.endswith('.json'):
            return self.parseJsonConfig(rawConfig)
        else:
            return self.parseLegacyConfig(rawConfig)

    def reloadConfig(self):
        """If the configuration file has changed, re-read it. If the parsed
        configuration object has changed, notify the coordinator."""
        fileStat = os.stat(self.filePath)
        if fileStat == self.lastFileStat:
            return
        self.lastFileStat = fileStat
        with open(self.filePath, 'rt') as f:
            rawConfig = f.read()
        config = self.parseConfig(rawConfig)
        if config != self.lastConfig:
            self.coordinator.onConfigUpdate(config)
            self.lastConfig = config


class HttpConfigurationObserver(FileConfigurationObserver):
    """ConfigurationObserver for configuration served over HTTP."""

    urlScheme = 'http://'

    def reloadConfig(self):
        dfd = client.getPage(self.configUrl)
        dfd.addCallbacks(self.onConfigReceived, self.logError)
        return dfd

    def onConfigReceived(self, rawConfig):
        config = self.parseConfig(rawConfig)
        if config != self.lastConfig:
            self.coordinator.onConfigUpdate(config)
            self.lastConfig = config
