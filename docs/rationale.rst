Rationale
=========

Why Arbalest?
-------------

* Lightweight library over heavyweight frameworks that can be composed with existing data tools
* Python is a `de facto <http://techblog.netflix.com/2013/03/python-at-netflix.html>`_ `lingua <https://pythonhosted.org/mrjob/>`_ `franca <http://blog.cloudera.com/blog/2013/01/a-guide-to-python-frameworks-for-hadoop/>`_ for data science
* Configuration as code
* Batteries included, for example, strategies for ingesting time series or sparse data (`arbalest.pipeline`), or integration with an existing pipeline topology (`arbalest.contrib`)

Use Cases
---------

Arbalest is not a MapReduce framework, but rather designed to make Amazon Redshift (and all its strengths) easy to use
with typical data workflows and tools. Here are a few examples:

* You are already using a `MapReduce <https://pythonhosted.org/mrjob/>`_ `framework <http://www.cascading.org/>`_ to process data in S3. Arbalest could make the results of an `Elastic MapReduce <https://aws.amazon.com/documentation/elastic-mapreduce/>`_ job queryable with SQL in Redshift. You can then hand off to Arbalest to define additional ETL in plain old SQL.
* You treat S3 as a catch all data sink, perhaps persisting JSON messages or events from a message system like `Kafka <https://github.com/pinterest/secor>`_ or RabbitMQ. Arbalest can expose some or all of this data into a data warehouse using Redshift. The ecosystem of SQL is now available for dashboards, reports, ad-hoc analysis.
* You have complex pipelines that could benefit from a fast, SQL queryable data sink. Arbalest has support out of the box (`arbalest.contrib`) to integrate with tools like `Luigi <https://github.com/spotify/luigi>`_ to be part of a multi-dependency, multi-step pipeline topology.
