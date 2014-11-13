#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name="pybal",
    version="0.1",
    description="PyBal LVS monitor",
    author="Mark Bergsma",
    author_email="mark@wikimedia.org",
    url="http://wikitech.wikimedia.org/view/Pybal",
    packages=['pybal', 'pybal.monitors'],
    requires=['twisted'],
    test_suite='pybal.test',
)
