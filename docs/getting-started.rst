Getting Started
===============

Installation
------------

    pip install arbalest

This will install Arbalest and any dependencies. However for Windows it may be
necessary to install `psycopg2`, a PostgreSQL database driver manually.

64 bit Python installation::

    pip install -e git+https://github.com/nwcell/psycopg2-windows.git@win64-py27#egg=psycopg2

32 bit Python installation::

    pip install -e git+https://github.com/nwcell/psycopg2-windows.git@win32-py27#egg=psycopg2

Concepts
--------

Pipelines
~~~~~~~~~

Arbalest orchestrates data loading using pipelines. Each `Pipeline`
can have one or many steps that are made up of three parts:

`metadata`: Path in an S3 bucket to store information needed for the copy process.

`source`: Path in an S3 bucket where data to be copied from is located consisting of JSON object files::

    { "id": "66bc8153-d6d9-4351-bada-803330f22db7", "someNumber": 1 }

`schema`: Definition of JSON objects to map into Redshift rows.

Schemas
~~~~~~~

A schema is defined using a `JsonObject` mapper which consists of one or many `Property` declarations.
By default the name of the JSON property is used as the column, but can be set
to a custom column name. Column names have a
`maximum length of 127 characters <http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html>`_. Column names
longer than 127 characters will be truncated.
Nested properties will create a default column name delimited by an underscore.

Example JSON Object (whitespace for clarity)::

    {
      "id": "66bc8153-d6d9-4351-bada-803330f22db7",
      "someNumber": 1,
      "child" : {
        "someBoolean": true
      }
    }

Example Schema:

.. code-block:: python

    JsonObject('destination_table_name',
        Property('id', 'VARCHAR(36)'),
        Property('someNumber', 'INTEGER', 'custom_column_name'),
        Property('child', Property('someBoolean', 'BOOLEAN')))

Copy Strategies
~~~~~~~~~~~~~~~

The `S3CopyPipeline` supports different strategies for copying data from S3 to Redshift.

**Bulk copy**

Bulk copy imports all keys in an S3 path into a Redshift table using a staging table.
By dropping and reimporting all data, duplication is eliminated.
This type of copy is useful for data that does not change very often or will
only be ingested once (e.g. immutable time series).

**Manifest copy**

A manifest copy imports all keys in an S3 path into a Redshift table using a `manifest <http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html>`_.
In addition, a journal of successfully imported objects is persisted to the `metadata` path.
Subsequent runs of this copy step will only copy S3 keys that do not exist in the journal.
This type of copy is useful for data in a path that changes often.

Example data copies:

.. code-block:: python

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
