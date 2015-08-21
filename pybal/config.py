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

from twisted.internet import inotify, defer, reactor, task
from twisted.names import client, dns
from twisted.python import failure, filepath


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
    """ConfigurationObserver for local configuration files."""

    urlScheme = 'file://'

    def __init__(self, coordinator, configUrl):
        self.coordinator = coordinator
        self.filePath = filepath.FilePath(configUrl[len(self.urlScheme):])
        self.notifier = inotify.INotify()
        self.notifier.startReading()
        self.notifier.watch(filepath.FilePath(self.filePath.dirname()),
                            mask=(inotify.IN_MODIFY| inotify.IN_ATTRIB),
                            callbacks=[self.onNotify])
        self.reloadConfig()

    def onNotify(self, ignored, filePath, mask):
        if filePath == self.filePath:
            try:
                self.reloadConfig()
            except Exception:
                logging.exception('Unable to reload config!')
                task.deferLater(reactor, 1, self.onNotify, ignored,
                                filePath, mask)

    def parseLegacyConfig(self, fp):
        """Parse a legacy (eval) configuration file."""
        config = {}
        for line in fp:
            try:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                server = ast.literal_eval(line)
                host = server['host']
                config[host] = server
            except (KeyError, SyntaxError, TypeError, ValueError):
                logging.exception('Bad configuration line: %s', line)
                continue
        return config

    def parseJsonConfig(self, fp):
        """Parse a JSON pool configuration file."""
        return json.load(fp)

    def reloadConfig(self):
        with self.filePath.open() as f:
            if self.filePath.path.endswith('.json'):
                config = self.parseJsonConfig(f)
            else:
                config = self.parseLegacyConfig(f)
        self.coordinator.onConfigUpdate(config)
