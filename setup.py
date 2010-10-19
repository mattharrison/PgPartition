# Copyright (c) 2010 Matt Harrison
#from distutils.core import setup
from setuptools import setup

from pgpartitionlib import meta

setup(name='PgPartition',
      version=meta.__version__,
      author=meta.__author__,
      description='FILL IN',
      scripts=['bin/pgpartition'],
      package_dir={'pgpartitionlib':'pgpartitionlib'},
      packages=['pgpartitionlib'],
)
