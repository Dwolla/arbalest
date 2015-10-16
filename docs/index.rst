.. arbalest documentation master file, created by
   sphinx-quickstart on Wed Oct 14 17:07:18 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

arbalest
========

**Arbalest is a Python data pipeline orchestration library for** `Amazon S3`_
**and** `Amazon Redshift`_.
**It takes care of the heavy lifting of making data queryable at scale in AWS.**

.. _Amazon S3: https://aws.amazon.com/documentation/s3/
.. _Amazon Redshift: https://aws.amazon.com/documentation/redshift/

It takes care of:

* Ingesting data into Amazon Redshift
* Schema creation and validation
* Creating highly available and scalable data import strategies
* Generating and uploading prerequisite artifacts for import
* Running data import jobs
* Orchestrating idempotent and fault tolerant multi-step ETL pipelines with SQL

Getting started is easy with ``pip``::

   pip install arbalest

Arbalest is licensed under the `MIT License`_.

.. _MIT License: https://github.com/Dwolla/arbalest/raw/master/LICENSE

Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
