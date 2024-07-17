
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
            |beam.Reshuffle()
        )
        read_data = (
            read_file_path
            |read_arvo_content(cdc_ignore_fields)
            |"bound windows arvo data" >> beam.WindowInto(FixedWindows(5)) # bound and sync with bq_schema
        )
        arvo_schema_convert = (
            read_file_path
            |arvo_schema_to_BQ_schema(cdc_ignore_fields)
            |"bound windows arvo schema" >> beam.WindowInto(FixedWindows(5))
            )
        bq_schema = (
            arvo_schema_convert
            |read_schema_from_BQ(project=bigquery_datalake.get('project'), data_set =bigquery_datalake.get('data_set'))
            |"bound windows bq schema" >> beam.WindowInto(FixedWindows(5))
        )
        schema_changes_handler=(
            ({'current_schema': arvo_schema_convert, 'exists_schema': bq_schema})
            |"pair cur-exists schema">> beam.CoGroupByKey()
            |schema_changes()
            # |"bound windows schema changes" >> beam.WindowInto(FixedWindows(5))
            
        )
        
        data_processing = (
            ({'data': read_data, 'bq_schema': schema_changes_handler})
            |"pair data - schema" >> beam.CoGroupByKey()
            |mapping_data_to_schema()
        )
        to_BQ = (
            data_processing
            |write_to_BQ(project=bigquery_datalake.get('project'), data_set =bigquery_datalake.get('data_set'))
        )