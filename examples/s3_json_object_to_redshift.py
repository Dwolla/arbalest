#!/usr/bin/env python
import psycopg2
from arbalest.configuration import env
from arbalest.redshift import S3CopyPipeline
from arbalest.redshift.schema import JsonObject, Property

"""
**Example: Bulk copy JSON objects from S3 bucket to Redshift table**

Arbalest orchestrates data loading using pipelines. Each `Pipeline`
can have one or many steps that are made up of three parts:

metadata: Path in an S3 bucket to store information needed for the copy process.

`s3://{BUCKET_NAME}/path_to_save_pipeline_metadata`

source: Path in an S3 bucket where data to be copied from is located.

`s3://{BUCKET_NAME}/path_of_source_data` consisting of JSON files:

```
{
  "id": "66bc8153-d6d9-4351-bada-803330f22db7",
  "someNumber": 1
}
```

schema: Definition of JSON objects to map into Redshift rows using a
`JsonObject` mapper which consists of one or many `Property` declarations.
By default the name of the JSON property is used as the column, but can be set
to a custom column name.
"""

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
