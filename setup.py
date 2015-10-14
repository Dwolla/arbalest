#!/usr/bin/env python
# encoding: utf-8

from setuptools import setup
import arbalest


setup(
    name='arbalest',
    version=arbalest.__version__,
    description='Arbalest orchestrates bulk data loading for Amazon Redshift',
    long_description=open('README.md').read(),
    author=arbalest.__author__,
    author_email='data+arbalest@dwolla.com',
    url='http://github.com/Dwolla/arbalest',
    install_requires=['boto>=2.32.1,<3.0',
                      'psycopg2'],
    tests_require=['mock==1.0.1'],
    packages=['arbalest', 'arbalest.redshift'],
    test_suite='test'
)
