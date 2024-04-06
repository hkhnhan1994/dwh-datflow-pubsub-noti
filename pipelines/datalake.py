import logging
import apache_beam as beam
from datalake.schema_beamSQL.schema  import (
    gcs_notification_message,
    )
import json, io, gzip
from google.cloud import storage
# print = logging.info

class pubsub_noti_messages(beam.PTransform):
    def __init__(self, subscription_path):
        self.subscription_path=subscription_path
    def expand(self, pcoll):
        def filter(element):
            return element.size != '0'
        def path_former(element):
            return "gs://"+element.bucket+"/"+element.name
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
                # | "to convert datatype" >> beam.Map(convert_to_string)
                | "schema writer1" >> beam.Map(lambda x:gcs_notification_message(**x)).with_output_types(gcs_notification_message)
                # | beam.Map(print)
        )
        get_message_contains_file_url =(
             convert_datatype
                | beam.Filter(filter)
                | beam.Map(path_former)
        )
        # windowing_time = (
        #      get_file_url | beam.WindowInto(beam.window.FixedWindows(2))
        # )
        
        # get_file_url | beam.Map(print)
        # transformations = (
        #     get_message_contains_file_url
        #      | "sql" >> SqlTransform("""SELECT name FROM PCOLLECTION""")
        #    )
        return  get_message_contains_file_url

class readfromtext(beam.DoFn):
    def setup(self):
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
        yield json.dumps(json_objects).encode('utf-8')

class read_file_from_gcs(beam.PTransform):
    def __init__(self,gcs_datalake):
        self.notification_config="gs://{}/{}/***.*".format(gcs_datalake.get('pucket'),gcs_datalake.get('sub_folder'))
    def expand(self, pcoll):
        def filter(element):
            return element['source_metadata']['table'] =='dwh_entity_role_properties'
        data = (
                pcoll
                # | beam.ParDo(readfromtext())
                | beam.io.ReadFromText(self.notification_config)
        )
        return  data
        # return data

class classify_table(beam.ptransform):
    def __init__(self,table_name):
        self.table_name=table_name
    def expand(self, pcoll):
        def filter(element,table_name):
            return element['source_metadata']['table'] == table_name
        data = (
                pcoll
                | beam.Map(json.loads)
                | beam.Filter(filter)
        )
        return  (data| beam.Map(print))
        # return data
