#!/usr/bin/env python

from setuptools import setup

setup(name='target-google-sheets',
      version='0.2.6',
      description='Singer.io target for writing data to Google Sheets',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_gsheet'],
      install_requires=[
          'jsonschema==2.6.0',
          'singer-python==1.5.0',
          'google-api-python-client==1.6.2',
          'backoff==1.3.2',
          'psutil==5.9.0'
      ],
      entry_points='''
          [console_scripts]
          target-google-sheets=target_gsheet:main
      ''',
)
