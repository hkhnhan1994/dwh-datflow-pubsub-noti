# import logging
import apache_beam as beam
import json
from avro.datafile import DataFileReader
from avro.io import DatumReader
import io
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
# print = logging.info

test_path='datastream-postgres/datastream/cmd_test/public_dwh_entity_role_properties/2024/05/22/08/40/7bb32d069d36f7728b1ca66f812bb4ff24413219_postgresql-backfill_-96622484_0_771.avro'
simulate_pubsub_mess={
  "name": test_path,
  "size": "1234",
  "bucket": "test_bucket_upvn",
}
mess=json.dumps(simulate_pubsub_mess, indent = 4) 

ignore_fields =[
    'stream_name',
    'schema_key',
    
]
class read_path(beam.PTransform):
    def __init__(self, subscription_path):
        self.subscription_path=subscription_path
    def expand(self, pcoll):
        def filter(element):
            return element['size'] != '0'
        def path_former(element):
            return "gs://"+element['bucket']+"/"+element['name']
        messages = (
                pcoll 
                # | "read gcs noti" >> beam.io.ReadFromPubSub(subscription=self.subscription_path).with_output_types(bytes) 
                | "Create data" >> beam.Create([mess])
        )
        # checking if pub/sub message contained any files
        get_message_contains_file_url =(
             messages
                | "to json" >> beam.Map(json.loads)
                |"check if file arrived" >> beam.Filter(filter)
                | beam.Map(path_former)
        )
        return get_message_contains_file_url
# as a side input
class read_arvo_schema(beam.PTransform):
    def __init__(self):
            self.fs = GCSFileSystem(PipelineOptions())
    def expand(self, pcoll):
        def read_schema(element):
            with self.fs.open(path=str(element)) as f:
                avro_bytes = f.read()
                f.close()    
            with io.BytesIO(avro_bytes) as avro_file:
                reader = DataFileReader(avro_file, DatumReader()).GetMeta('avro.schema')
                parsed = json.loads(reader)
                avro_file.close()
                # return (json.dumps(parsed, indent=4, sort_keys=True))
                return parsed
        schema = ( # data must be in Arvo format
            pcoll
            | beam.Map(read_schema)
        )
        # result =(
        #     {'schema':schema,'data':data}
        #     |beam.Map(extract_result)
        # )
        # return  result
        return schema
class read_arvo_content(beam.PTransform):
    def expand(self, pcoll):
        data = ( # data must be in Arvo format
            pcoll
            | beam.io.ReadAllFromAvro()
        )
        return data

class gcs_arvo_processing(beam.PTransform):
    def __init__(self, subscription_path):
        self.subscription_path=subscription_path
    def expand(self, pcoll):
        def remove_unrelated(element):
            for key in keys_to_remove:
                with suppress(KeyError):
                    del element[0][key]
            element[0].pop('read_timestamp')
            return element
        def cross_join(left, rights):
            for x in rights:
                yield (left, x)
        path =(
            pcoll| read_path(self.subscription_path)
        )
        data = (path| read_arvo_content())
        schema = (path| read_arvo_schema())
        Joined_schema = (
            data
            | 'ApplyCrossJoin' >> beam.FlatMap(
                cross_join, rights=beam.pvalue.AsIter(schema)))
        remove_unused_fields =(
            Joined_schema |beam.Map(remove_unrelated)
        )
        return remove_unused_fields
'''
# class flatten_streaming_file(beam.PTransform):
#     def __init__(self, subscription_path):
#         self.subscription_path=subscription_path
#     def expand(self, pcoll):
        
                 
class classify_table(beam.PTransform):
    def __init__(self,table_name):
        self.table_name=table_name
    def expand(self, pcoll):
        def filter(element,table_name):
            return element['source_metadata']['table'] == table_name
        data = (
                pcoll
                | beam.Map(json.loads)
                | beam.Filter(filter, self.table_name)
        )
        return  data





# '''
# treat payload as a string
# load data into schema =>
# load and get the metadata
# classify by table name
# add the schema once again 
# '''