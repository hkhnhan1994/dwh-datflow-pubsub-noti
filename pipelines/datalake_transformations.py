# import logging
import apache_beam as beam
import json
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





'''
treat payload as a string
load data into schema =>
load and get the metadata
classify by table name
add the schema once again 
'''