#!/usr/bin/env python

from distutils.core import setup

setup(name="pybal",
    version="0.1",
    description="PyBal LVS monitor",
    author="Mark Bergsma",
    author_email="mark@nedworks.org",
    url="http://svn.wikimedia.org/viewvc/svnroot/mediawiki/trunk/pybal",
    packages = ['pybal', 'pybal.monitors'])
