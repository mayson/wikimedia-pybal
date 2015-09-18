"""
util.py - PyBal utility classes
Copyright (C) 2006-2008 by Mark Bergsma <mark@nedworks.org>

LVS Squid balancer/monitor for managing the Wikimedia Squid servers using LVS
"""
import sys
import datetime


def get_subclasses(cls):
    """Return a list of all direct and indirect subclasses of a given class."""
    subclasses = []
    for subclass in cls.__subclasses__():
        subclasses.append(subclass)
        subclasses.extend(get_subclasses(subclass))
    return subclasses


class ConfigDict(dict):

    def getint(self, key, default=None):
        try:
            return int(self[key])
        except KeyError:
            if default is not None:
                return default
            else:
                raise
        # do not intercept ValueError

    def getboolean(self, key, default=None):
        try:
            value = self[key]
        except KeyError:
            if default is not None:
                return default
            else:
                raise
        else:
            if value in (True, False):
                return value
            value = value.strip().lower()
            if value in ('t', 'true', 'y', 'yes', 'on', '1'):
                return True
            elif value in ('f', 'false', 'n', 'no', 'off', '0'):
                return False
            else:
                raise ValueError

    def getfloat(self, key, default=None):
        try:
            return float(self[key])
        except KeyError:
            if default is not None:
                return default
            else:
                raise
        # do not intercept ValueError
