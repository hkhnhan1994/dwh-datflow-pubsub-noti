
import logging
import apache_beam as beam
from .datalake_transformations import  gcs_arvo_processing, write_to_BQ
from config.develop import cdc_ignore_fields, pubsub_config, bigquery_datalake
# from util.schema import read_datalake_schema, register_beam_coder
# print = logging.info

def run(beam_options):
    with beam.Pipeline(options=beam_options) as p:
        
        _ = (
            p
                | gcs_arvo_processing(pubsub_project=pubsub_config.get('project'),pubsub_subscription=pubsub_config.get('subscription'),ignore_fields=cdc_ignore_fields)
                | write_to_BQ(project=bigquery_datalake.get('project'), data_set =bigquery_datalake.get('data_set') )
                # | beam.Map(print)
        )
        