from apache_beam.options.pipeline_options import PipelineOptions
import logging
import apache_beam as beam
from datalake.schema_beamSQL.schema  import (
    gcs_notification_message,
    )
from apache_beam.transforms.sql import SqlTransform
import json, io, gzip
from util.processing import convert_to_string
from google.cloud import storage

class pubsub_noti_messages(beam.PTransform):
    def __init__(self, subscription_path):
        self.subscription_path=subscription_path
    def expand(self, pcoll):
        def filter(element):
            return element.size != '0'
        messages = (
                pcoll
                # | "read gcs noti" >> beam.io.ReadFromText('/home/hkhnhan/Code/dwh-datflow-pubsub-noti/dwh_entity_role_properties.jsonl')
                # | "read gcs noti" >> beam.io.ReadFromPubSub(subscription="projects/pj-bu-dw-data-sbx/subscriptions/gcs_noti_sub").with_output_types(bytes)
                # | "read gcs noti" >> beam.io.ReadFromPubSub(subscription=self.subscription_path).with_output_types(bytes) 
                | "read gcs noti" >> beam.io.ReadFromText('/home/hkhnhan/Code/dwh-datflow-pubsub-noti/pubsub_noti.jsonl')
                | beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes)
        )
        # lines = (
        #     messages | "decode messages" >> beam.Map(lambda x: x.decode('utf-8'))
        # )
        convert_datatype =(
             messages 
                | "to json" >> beam.Map(json.loads)
                | "to convert datatype" >> beam.Map(convert_to_string)
                | "schema writer1" >> beam.Map(lambda x:gcs_notification_message(**x)) #.with_output_types(gcs_notification_message)
                # | "schema writer2" >> beam.Map(lambda x:x).with_output_types(gcs_notification_message)
                # | beam.Map(print)
        )
        get_message_contains_file_url =(
             convert_datatype
                | beam.Filter(filter)
                # | beam.Map(lambda x:x.name)
        )
        # windowing_time = (
        #      get_file_url | beam.WindowInto(beam.window.FixedWindows(2))
        # )
        
        # get_file_url | beam.Map(print)
             
            #  | "sql" >> SqlTransform("""SELECT name FROM PCOLLECTION""")
        return  get_message_contains_file_url

class readfromtext(beam.DoFn):
    def setup(self):
    # Called whenever the DoFn instance is deserialized on the worker.
    # This means it can be called more than once per worker because multiple instances of a given DoFn subclass may be created (e.g., due to parallelization, or due to garbage collection after a period of disuse).
    # This is a good place to connect to database instances, open network connections or other resources.
        self.storage_client = storage.Client()
    def process(self, element):
        # path = "gs://"+element.bucket+"/"+element.name
        bucket = self.storage_client.get_bucket(element.bucket)
        blob = bucket.blob(element.name)
        if element.name.split('/')[-1].endswith('gz'):
            blob=io.BytesIO(blob.download_as_string())
            with gzip.open(blob, 'rb') as gz:
            #     # Read compressed file as a file object
                file = gz.read().decode('utf-8')
        else:
            file=blob.download_as_string()
        json_objects = file.strip().split('\n')
        # print(json_objects[0])
        # obj = json_objects[0]
        # print(json_objects)
        yield json.dumps(json_objects).encode('utf-8')

        # yield io.BytesIO(blob.download_as_string())
        # with 
        # yield blob.download_to_file()

class read_file_from_gcs(beam.PTransform):
    def __init__(self):
        self.path = ''#"gs://test_bucket_upvn/datastream-postgres/datastream/cmd_test/public_dwh_natural_persons/2024/04/05/06/45/6d6da8584e972a7458112e3738a4c01d3ef392c1_postgresql-backfill_881162559_7_35668.jsonl.gz"
    def expand(self, pcoll):
        def get_path(element):
            self.path = "gs://"+element.bucket+"/"+element.name
            # print(self.path)
            return beam.io.ReadAllFromText(self.path)
        data = (
                pcoll
                | beam.ParDo(readfromtext())
                | beam.Map(json.loads)
                # | beam.Map(print)
                # | beam.Map(lambda x: x.decode('utf-8'))    
                # |beam.WindowInto(beam.window.FixedWindows(2))
                # | "read gcs noti" >> beam.io.ReadFromText(self.path)
        )
        # lines = (
        #     messages | "decode messages" >> beam.Map(lambda x: x.decode('utf-8'))
        # )
        
        # return  (data| beam.Map(print))