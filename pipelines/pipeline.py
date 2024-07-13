
import logging
import apache_beam as beam
from .datalake_transformations import  mapping_data_to_schema, write_to_BQ, read_path_from_pubsub, arvo_schema_to_BQ_schema, read_arvo_content, schema_changes, read_schema_from_BQ
from config.develop import cdc_ignore_fields, pubsub_config, bigquery_datalake
from apache_beam.transforms.window import FixedWindows
# print = logging.info

def run(beam_options):
    with beam.Pipeline(options=beam_options) as p:
        read_file_path = (
            p
            | read_path_from_pubsub(project=pubsub_config.get('project'),subscription=pubsub_config.get('subscription'))
        )
        read_data = (
            read_file_path 
            |read_arvo_content(cdc_ignore_fields)   
        )
        arvo_schema_convert = (
            read_file_path
            |arvo_schema_to_BQ_schema(cdc_ignore_fields)
            )
        bq_schema = (
            arvo_schema_convert
            |read_schema_from_BQ(project=bigquery_datalake.get('project'), data_set =bigquery_datalake.get('data_set'))
            |"bound windows bq schema" >> beam.WindowInto(FixedWindows(5))
            # |beam.Map(print)
        )
        schema_changes_handler=(
           arvo_schema_convert
            |"bound windows arvo schema" >> beam.WindowInto(FixedWindows(5))
            |schema_changes(project=bigquery_datalake.get('project'), data_set =bigquery_datalake.get('data_set'),bq_schema=bq_schema)
            |"bound windows schema changes" >> beam.WindowInto(FixedWindows(5))
        )
        
        data_processing = (
            read_data
            |"bound windows arvo data" >> beam.WindowInto(FixedWindows(5)) # bound and sync with bq_schema
            |mapping_data_to_schema(bq_schema=schema_changes_handler)
            # |beam.Map(print) 
            |"bound windows data processing" >> beam.WindowInto(FixedWindows(5)) # extra sync with write_to_BQ
        )
        await_schema_bound =(
            schema_changes_handler
            |"bound windows schema" >> beam.WindowInto(FixedWindows(5)) # extra sync with write_to_BQ
        )
        to_BQ = (
            data_processing
            |beam.Reshuffle()
            |write_to_BQ(project=bigquery_datalake.get('project'), data_set =bigquery_datalake.get('data_set'), bq_schema=await_schema_bound)
        )