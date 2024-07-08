
import logging
import apache_beam as beam
from .datalake_transformations import  gcs_arvo_processing, write_to_BQ, read_path_from_pubsub, arvo_schema_processing, read_arvo_content
from config.develop import cdc_ignore_fields, pubsub_config, bigquery_datalake
from apache_beam.transforms.window import FixedWindows
# from util.schema import read_datalake_schema, register_beam_coder
# print = logging.info

def run(beam_options):
    with beam.Pipeline(options=beam_options) as p:
        
        read_file_path = (
            p
            | read_path_from_pubsub(project=pubsub_config.get('project'),subscription=pubsub_config.get('subscription'))
            # | beam.Map(print)
        )
        bq_schema = (
            read_file_path
            |arvo_schema_processing(cdc_ignore_fields)
            |"bound windows schema" >> beam.WindowInto(FixedWindows(5))
            # | beam.Map(print)
            )
        data_processing = (
            read_file_path 
            |read_arvo_content(cdc_ignore_fields)
            |"bound windows arvo data" >> beam.WindowInto(FixedWindows(5))
            |gcs_arvo_processing(bq_schema=bq_schema )
            |"bound windows data processing" >> beam.WindowInto(FixedWindows(5))
            # | beam.Map(print)
        )
        
        to_BQ = (
            data_processing
            |write_to_BQ(project=bigquery_datalake.get('project'), data_set =bigquery_datalake.get('data_set'), bq_schema=bq_schema)
        )