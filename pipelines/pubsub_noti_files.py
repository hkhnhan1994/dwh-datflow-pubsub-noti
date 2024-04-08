# import logging
import apache_beam as beam
from datalake.schema_beamSQL.schema  import (
    gcs_notification_message,
    )
import json
# import io, gzip
# from google.cloud import storage
# print = logging.info

class pubsub_noti_messages(beam.PTransform):
    def __init__(self, subscription_path):
        self.subscription_path=subscription_path
    def expand(self, pcoll):
        def filter(element):
            return element['size'] != '0'
        def path_former(element):
            return "gs://"+element['bucket']+"/"+element['name']
        messages = (
                pcoll
                # | "read gcs noti" >> beam.io.ReadFromText('/home/hkhnhan/Code/dwh-datflow-pubsub-noti/dwh_entity_role_properties.jsonl')
                # | "read gcs noti" >> beam.io.ReadFromPubSub(subscription="projects/pj-bu-dw-data-sbx/subscriptions/gcs_noti_sub").with_output_types(bytes)
                # | "read gcs noti" >> beam.io.ReadFromPubSub(subscription=self.subscription_path).with_output_types(bytes) 
                | "read gcs noti" >> beam.io.ReadFromText('/home/hkhnhan/Code/dwh-datflow-pubsub-noti/pubsub_noti.jsonl')
                | beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes)
        )
        get_message_contains_file_url =(
             messages
                | "to json" >> beam.Map(json.loads)
                |"check if file arrived" >> beam.Filter(filter)
                | beam.Map(path_former)
        )
        data = (
            get_message_contains_file_url
            | beam.io.ReadAllFromText()
        )
        return  data

