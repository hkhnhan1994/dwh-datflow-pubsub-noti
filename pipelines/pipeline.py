
import logging
import apache_beam as beam
from .transformations import  write_to_BQ, read_path_from_pubsub, arvo_schema, read_arvo_content
from .functions import  merge_schema, read_bq_schema, create_table, enrich_data
from config.develop import cdc_ignore_fields, pubsub_config, bigquery_datalake
from apache_beam.transforms.window import FixedWindows

# print = logging.info

def run(beam_options):
    with beam.Pipeline(options=beam_options) as p:
        read_file_path = (
            p
            | read_path_from_pubsub(project=pubsub_config.get('project'),subscription=pubsub_config.get('subscription'))
            # |beam.Map(print)
        )
        read_data = (
            read_file_path
            |read_arvo_content(cdc_ignore_fields)
            |"bound windows arvo data" >> beam.WindowInto(FixedWindows(5)) # bound and sync with bq_schema
            |beam.Map(print)
        )
        # schema_processing = (
        #     read_file_path
        #     |arvo_schema(cdc_ignore_fields)
        #     |beam.ParDo(read_bq_schema(project=bigquery_datalake.get('project'), dataset =bigquery_datalake.get('dataset')))
        #     |beam.ParDo(merge_schema())
        #     |beam.ParDo(create_table(project=bigquery_datalake.get('project'), dataset =bigquery_datalake.get('dataset')))
        #     |beam.Reshuffle()
        #     |"bound windows schema" >> beam.WindowInto(FixedWindows(5))
        #     )
        
        # data_processing = (
        #     ({'data': read_data, 'bq_schema': schema_processing})
        #     |"pair data - schema" >> beam.CoGroupByKey()
        #     |beam.ParDo(enrich_data())
        # )
        # to_BQ = (
        #     data_processing
        #     |write_to_BQ(project=bigquery_datalake.get('project'), dataset =bigquery_datalake.get('dataset'))
        # )