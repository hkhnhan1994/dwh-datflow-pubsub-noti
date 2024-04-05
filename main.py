from apache_beam.options.pipeline_options import PipelineOptions
import logging
import apache_beam as beam
from datalake.schema_beamSQL.schema  import (
    gcs_notification_message,
    cdc_dwh_entity_role_properties,
    )
from apache_beam.transforms.sql import SqlTransform
import json
from config.pipeline.dev import config
from pubsub_noti_files import pubsub_noti_messages, read_file_from_gcs
# print = logging.info
def run(beam_options):
    with beam.Pipeline(options=beam_options) as p:
        message_file_path = (
            p| pubsub_noti_messages("projects/pj-bu-dw-data-sbx/subscriptions/gcs_noti_sub")
        )
        data_files = (
            message_file_path | read_file_from_gcs()
        )
        
if __name__ == '__main__':
    # Run the pipeline.
    logging.getLogger(__name__).setLevel(logging.INFO)
    beam_options = PipelineOptions.from_dictionary(config)
    run(beam_options)


