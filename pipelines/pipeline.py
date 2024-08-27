"""Main pipline for structured data lake."""
import apache_beam as beam
from .transformations import  write_to_BQ, read_path_from_pubsub, schema_processing, read_avro_content,map_new_data_to_bq_schema, write_error_to_alert
from config.develop import cdc_ignore_fields, pubsub_config, bigquery_datalake, dead_letter
from apache_beam.transforms.window import FixedWindows
from config.develop import print_debug,print_error,print_info

def run(beam_options):
    """Forming the pipeline."""
    with beam.Pipeline(options=beam_options) as p:
        read_file_path = (
            p
            | read_path_from_pubsub(pubsub_config=pubsub_config)
            # |beam.Map(print_debug)
        )
        data = (
            read_file_path
            |read_avro_content(ignore_fields = cdc_ignore_fields)
            # |beam.Map(print_debug)
        )
        data_windows = (
            data.data
            |"bound windows arvo data" >> beam.WindowInto(FixedWindows(3)) # bound and sync with bq_schema
        )
        data_error_windows =(
            data.error
            |"bound windows arvo data error" >> beam.WindowInto(FixedWindows(3)) # bound and sync with bq_schema
        )
        schema, schema_error = (
            read_file_path
            |schema_processing(cdc_ignore_fields,bq_pars=bigquery_datalake)
            # |beam.Map(print_debug)
            )
        schema_windows =(
            schema
            # |beam.Reshuffle()
            |"bound windows schema" >> beam.WindowInto(FixedWindows(3))
            # |beam.Map(print_debug)
        )
        schema_error_windows=(
            schema_error
            |"bound windows errors schema_error" >> beam.WindowInto(FixedWindows(3))
        )
        data_processing, data_processing_error = (
            ({'data': data_windows, 'bq_schema': schema_windows})
            |"pair data - schema" >> beam.CoGroupByKey()
            |map_new_data_to_bq_schema()
        )
        data_processing_error_windows=(
            data_processing_error
            |"bound windows errors data_processing" >> beam.WindowInto(FixedWindows(3))
        )
        to_BQ = (
            data_processing
            |beam.Reshuffle()
            |write_to_BQ()
        )
        to_BQ_error_windows = (
            to_BQ
            |"bound windows errors to_BQ" >> beam.WindowInto(FixedWindows(3))
        )
        errors =(
            (to_BQ_error_windows,data_processing_error_windows,schema_error_windows,data_error_windows )
            | write_error_to_alert(dead_letter) 
        ) 
  
        