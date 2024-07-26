"""Main pipline for structured data lake."""

import logging
import apache_beam as beam
from .transformations import  write_to_BQ, read_path_from_pubsub, schema_processing, read_avro_content, write_error_to_alert
from .functions import enrich_data
from config.develop import cdc_ignore_fields, pubsub_config, bigquery_datalake, dead_letter
from apache_beam.transforms.window import FixedWindows

print = logging.info

def run(beam_options):
    """Forming the pipeline."""
    with beam.Pipeline(options=beam_options) as p:
        read_file_path = (
            p
            | read_path_from_pubsub(project=pubsub_config.get('project'),subscription=pubsub_config.get('subscription'))
            # |beam.Map(print)
        )
        data = (
            read_file_path
            |read_avro_content(ignore_fields = cdc_ignore_fields, error_handler = dead_letter)
            |"bound windows arvo data" >> beam.WindowInto(FixedWindows(5)) # bound and sync with bq_schema
            # |beam.Map(print)
        )
        
        schema = (
            read_file_path
            |schema_processing(cdc_ignore_fields,project=bigquery_datalake.get('project'),dataset=bigquery_datalake.get('dataset'),error_handler = dead_letter )
            |beam.Reshuffle()
            |"bound windows schema" >> beam.WindowInto(FixedWindows(5))
            # |beam.Map(print)
            )
        data_processing = (
            ({'data': data, 'bq_schema': schema})
            |"pair data - schema" >> beam.CoGroupByKey()
            |beam.ParDo(enrich_data()).with_outputs('error', main='data')
        )
        to_BQ = (
            data_processing.data
            |write_to_BQ(project=bigquery_datalake.get('project'), dataset =bigquery_datalake.get('dataset'))
        )
        errors =(
            (to_BQ, data_processing.error) | write_error_to_alert(dead_letter) 
        ) 
        