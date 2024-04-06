from apache_beam.options.pipeline_options import PipelineOptions
import logging
import apache_beam as beam
from .datalake import  read_file_from_gcs
# print = logging.info
def run(beam_options,gcs_datalake):
    with beam.Pipeline(options=beam_options) as p:
        # message_file_path = (
        #     p
        #         | pubsub_noti_messages("projects/pj-bu-dw-data-sbx/subscriptions/gcs_noti_sub")
        #         | beam.Map(print)
        # )
        read_from_gcs = (
            p | read_file_from_gcs(gcs_datalake)
        )
        classify_table = (
            
        )