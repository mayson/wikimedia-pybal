"""
pybal.__init__.py
Copyright (C) 2006 by Mark Bergsma <mark@nedworks.org>

The pybal package contains all PyBal modules
"""
import test

__version__ = '1.6'

USER_AGENT_STRING = 'PyBal/%s' % __version__

__all__ = ('ipvs', 'monitor', 'pybal', 'util', 'monitors', 'bgp',
           'USER_AGENT_STRING')
