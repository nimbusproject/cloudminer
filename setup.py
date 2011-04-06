#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import sys

py_version = 2.5
Version=0.2

if float("%d.%d" % sys.version_info[:2]) < py_version:
    sys.stderr.write("Your Python version %d.%d.%d is not supported.\n" % sys.version_info[:3])
    sys.stderr.write("cloudminer requires Python %f or newer.\n" % (py_version))
    sys.exit(1)

setup(name='cloudminer',
      version=Version,
      description='Persistence API for the cloudyvents API.',
      author='Nimbus Team',
      author_email='nimbus@mcs.anl.gov',
      url='http://www.nimbusproject.org/',
      packages=[ 'cloudminer', 'cloudminer.tests' ],
      long_description="",
      license="Apache2",
      install_requires = ["nose", "cloudyvents"],
     )
