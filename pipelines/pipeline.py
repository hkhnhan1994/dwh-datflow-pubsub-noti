
import logging
import apache_beam as beam
from .pubsub_noti_files import  pubsub_noti_messages
import json
# print = logging.info
def run(beam_options,gcs_datalake):
    with beam.Pipeline(options=beam_options) as p:
        
        message_file_path = (
            p
                | pubsub_noti_messages("projects/pj-bu-dw-data-sbx/subscriptions/gcs_noti_sub")
                | beam.Map(print)
        )