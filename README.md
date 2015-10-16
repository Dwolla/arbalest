arbalest
========

Arbalest is a Python data pipeline orchestration library for
[Amazon S3](https://aws.amazon.com/documentation/s3/)
and [Amazon Redshift](https://aws.amazon.com/documentation/redshift/).
It takes care of the heavy lifting of making data queryable at scale in AWS.

It takes care of:

* Ingesting data into Amazon Redshift
* Schema creation and validation
* Creating highly available and scalable data import strategies
* Generating and uploading prerequisite artifacts for import
* Running data import jobs
* Orchestrating idempotent and fault tolerant multi-step ETL pipelines with SQL

**Why Arbalest?**

* Lightweight library over heavyweight frameworks that can be composed with existing data tools
* Python is a [de facto](http://techblog.netflix.com/2013/03/python-at-netflix.html)
[lingua](https://pythonhosted.org/mrjob/)
[franca](http://blog.cloudera.com/blog/2013/01/a-guide-to-python-frameworks-for-hadoop/)
for data science
* Configuration as code
* Batteries included, for example, strategies for ingesting time series
or sparse data (`arbalest.pipeline`), or integration with an existing pipeline topology (`arbalest.contrib`)

**Use cases**

Arbalest is not a MapReduce framework, but rather designed to make Amazon Redshift (and all its strengths) easy to use
with typical data workflows and tools. Here are a few examples:

* You are already using a [MapReduce](https://pythonhosted.org/mrjob/) [framework](http://www.cascading.org/) to process data in S3.
Arbalest could make the results of an [Elastic MapReduce](https://aws.amazon.com/documentation/elastic-mapreduce/) job queryable with SQL in Redshift.
You can then hand off to Arbalest to define additional ETL in plain old SQL.
* You treat S3 as a catch all data sink, perhaps persisting JSON messages or events from a message system like [Kafka](https://github.com/pinterest/secor) or RabbitMQ.
Arbalest can expose some or all of this data into a data warehouse using Redshift. The ecosystem of SQL is now available for dashboards, reports, ad-hoc analysis.
* You have complex pipelines that could benefit from a fast, SQL queryable data sink.
Arbalest has support out of the box (`arbalest.contrib`) to integrate with tools like [Luigi](https://github.com/spotify/luigi) to be part of a multi-dependency, multi-step pipeline topology.

## Getting Started

Getting started is easy with `pip`:

`pip install arbalest`

Examples of Arbalest pipeline are in `examples/`. An overview of concepts and classes are below.

*Note*

Arbalest depends on psycopg2. However, installing psycopg2 on Windows may not be straight forward.

To install psycopg2 on Windows:

64 bit Python installation:

```
pip install -e git+https://github.com/nwcell/psycopg2-windows.git@win64-py27#egg=psycopg2
```

32 bit Python installation:

```
pip install -e git+https://github.com/nwcell/psycopg2-windows.git@win32-py27#egg=psycopg2
```

## Pipelines

Arbalest orchestrates data loading using pipelines. Each `Pipeline`
can have one or many steps that are made up of three parts:

`metadata`: Path in an S3 bucket to store information needed for the copy process.

`source`: Path in an S3 bucket where data to be copied from is located consisting of JSON object files:

```
{ "id": "66bc8153-d6d9-4351-bada-803330f22db7", "someNumber": 1 }
```

`schema`: Definition of JSON objects to map into Redshift rows.

## Schemas

A schema is defined using a `JsonObject` mapper which consists of one or many `Property` declarations.
By default the name of the JSON property is used as the column, but can be set
to a custom column name. Column names have a
[maximum length of 127 characters](http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html). Column names
longer than 127 characters will be truncated.
Nested properties will create a default column name delimited by an underscore.

Example JSON Object (whitespace for clarity):

```
{
  "id": "66bc8153-d6d9-4351-bada-803330f22db7",
  "someNumber": 1,
  "child" : {
    "someBoolean": true
  }
}
```

Example Schema:

```python
JsonObject('destination_table_name',
    Property('id', 'VARCHAR(36)'),
    Property('someNumber', 'INTEGER', 'custom_column_name'),
    Property('child', Property('someBoolean', 'BOOLEAN')))
```

## Copy strategies

The `S3CopyPipeline` supports different strategies for copying data from S3 to Redshift.

### Bulk copy

Bulk copy imports all keys in an S3 path into a Redshift table using a staging table.
By dropping and reimporting all data, duplication is eliminated.
This type of copy is useful for data that does not change very often or will
only be ingested once (e.g. immutable time series).

### Manifest copy

A manifest copy imports all keys in an S3 path into a Redshift table using a [manifest](http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html).
In addition, a journal of successfully imported objects is persisted to the `metadata` path.
Subsequent runs of this copy step will only copy S3 keys that do not exist in the journal.
This type of copy is useful for data in a path that changes often.

```python
#!/usr/bin/env python
import psycopg2
from arbalest.configuration import env
from arbalest.redshift import S3CopyPipeline
from arbalest.redshift.schema import JsonObject, Property

if __name__ == '__main__':
    pipeline = S3CopyPipeline(
        aws_access_key_id=env('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=env('AWS_SECRET_ACCESS_KEY'),
        bucket=env('BUCKET_NAME'),
        db_connection=psycopg2.connect(env('REDSHIFT_CONNECTION')))

    pipeline.bulk_copy(metadata='path_to_save_pipeline_metadata',
                       source='path_of_source_data',
                       schema=JsonObject('destination_table_name',
                                         Property('id', 'VARCHAR(36)'),
                                         Property('someNumber', 'INTEGER',
                                                  'custom_column_name')))

    pipeline.manifest_copy(metadata='path_to_save_pipeline_metadata',
                           source='path_of_incremental_source_data',
                           schema=JsonObject('incremental_destination_table_name',
                                             Property('id', 'VARCHAR(36)'),
                                             Property('someNumber', 'INTEGER',
                                                      'custom_column_name')))

    pipeline.run()
```

## SQL

Pipelines can also have arbitrary SQL steps. Each SQL step can have one or many statements which are executed in a transaction. Expanding on the previous example:

```python
#!/usr/bin/env python
import psycopg2
from arbalest.configuration import env
from arbalest.redshift import S3CopyPipeline
from arbalest.redshift.schema import JsonObject, Property

if __name__ == '__main__':
    pipeline = S3CopyPipeline(
        aws_access_key_id=env('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=env('AWS_SECRET_ACCESS_KEY'),
        bucket=env('BUCKET_NAME'),
        db_connection=psycopg2.connect(env('REDSHIFT_CONNECTION')))

    pipeline.bulk_copy(metadata='path_to_save_pipeline_metadata',
                       source='path_of_source_data',
                       schema=JsonObject('destination_table_name',
                                         Property('id', 'VARCHAR(36)'),
                                         Property('someNumber', 'INTEGER',
                                                  'custom_column_name')))

    pipeline.manifest_copy(metadata='path_to_save_pipeline_metadata',
                           source='path_of_incremental_source_data',
                           schema=JsonObject('incremental_destination_table_name',
                                             Property('id', 'VARCHAR(36)'),
                                             Property('someNumber', 'INTEGER',
                                                      'custom_column_name')))

    pipeline.sql(('SELECT someNumber + %s '
                  'INTO some_olap_table FROM destination_table_name', 1),
                 ('SELECT * INTO destination_table_name_copy '
                  'FROM destination_table_name'))

    pipeline.run()
```

## Orchestration Helpers

Included in this project are a variety of orchestration helpers to assist with
the creation of pipelines.
These classes are defined in the `arbalest.pipeline` and `arbalest.contrib` modules.

### Sorted data sources

Assuming source data is stored in a sortable series of directories, `S3SortedDataSources`
facilitates the retrieval of S3 paths in a sequence for import, given a start
and/or end. In addition, it has methods to mark a cursor in an S3 persisted journal.

Examples of sorted series:

Sequential integers

```
s3://bucket/child/1/*
s3://bucket/child/2/*
s3://bucket/child/3/*
```

Time series:

```
s3://bucket/child/2015-01-01/*
s3://bucket/child/2015-01-02/*
s3://bucket/child/2015-01-03/*
s3://bucket/child/2015-01-04/00/*
```

### Time series

`SqlTimeSeriesImport` implements a bulk copy and update strategy of data from
a list of time series sources from `S3SortedDataSources` into an existing
target table.

### Luigi

`PipelineTask` wraps any `arbalest.core.Pipeline` into a
[Luigi Task](http://luigi.readthedocs.org/en/latest/api_overview.html?highlight=task#task).
This allows for the composition of workflows with dependency graphs, for example,
data pipelines that are dependent on multiple steps or other pipelines. Luigi then takes care of
the heavy lifting of
[scheduling and executing](http://luigi.readthedocs.org/en/latest/central_scheduler.html)
multistep pipelines.

## License

Arbalest is licensed under the [MIT License](https://github.com/Dwolla/arbalest/raw/master/LICENSE).

## Authors and Contributors

Arbalest was built at Dwolla, primarily by [Fredrick Galoso](https://github.com/wayoutmind).
Initial support for Luigi and contributions to orchestration helpers by [Hayden Goldstien](https://github.com/hgoldsti).
We gladly welcome contributions and feedback. If you are using Arbalest we would love to know.
