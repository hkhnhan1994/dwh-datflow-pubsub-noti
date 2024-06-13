
import logging
import apache_beam as beam
from .datalake_transformations import  gcs_arvo_processing, write_to_BQ
# from util.schema import read_datalake_schema, register_beam_coder
# print = logging.info

def run(beam_options):
    with beam.Pipeline(options=beam_options) as p:
        
        message_file_path = (
            p
                | gcs_arvo_processing("projects/pj-bu-dw-data-sbx/subscriptions/gcs_noti_sub")
                | write_to_BQ()
                # | beam.Map(print)
        )
        # tables = (
        #     message_file_path 
        #         | classify_table("dwh_entity_role_properties")
        #         | beam.Map(print)
        # )
        