#!/usr/bin/env python
# encoding: utf-8

from setuptools import setup
import arbalest

setup(
    name='arbalest',
    version=arbalest.__version__,
    description='Arbalest orchestrates bulk data loading for Amazon Redshift',
    long_description=open('README.rst').read(),
    author=arbalest.__author__,
    author_email='fred+arbalest@dwolla.com',
    license='MIT',
    url='https://github.com/Dwolla/arbalest',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: System :: Distributed Computing'
    ],
    install_requires=['boto>=2.32.1,<3.0',
                      'luigi==1.0.20',
                      'protobuf==2.6.1',
                      'psycopg2'],
    tests_require=['mock==1.0.1'],
    packages=['arbalest',
              'arbalest.contrib',
              'arbalest.pipeline',
              'arbalest.redshift'],
    test_suite='test'
)
