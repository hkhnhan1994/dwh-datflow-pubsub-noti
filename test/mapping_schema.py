from apache_beam.options.pipeline_options import PipelineOptions
import logging
import apache_beam as beam
from datalake.schema_beamSQL.schema  import (
    cdc_dwh_entity_role_properties
    )
from apache_beam.transforms.sql import SqlTransform
import json
# print = logging.info
config ={
    "job_name": "cmd-stream1",
    "staging_location": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/staging",
    "temp_location": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/temp",
    "project": "pj-bu-dw-data-sbx",
    "region": "europe-west1",
    "max_num_workers": 1,
    "num_workers": 1,
    "worker_region": 'europe-west1',
    "machine_type": 'n1-standard-1',
    "disk_size_gb": 10,
    "runner": "DirectRunner", # DirectRunner DataflowRunner
    "setup_file": './setup.py',
    "save_main_session" : True,
    "streaming":True
}


    # return beamSQL_schema.gcs_notification_message(**message)
def convert_to_string(data):
    if isinstance(data, list):
        return [convert_to_string(item) for item in data]
    elif isinstance(data, dict):
        return {key: convert_to_string(value) for key, value in data.items()}
    else:
        return str(data)
# def mapping_schema(data):

#     data_fields= (field for field in data.items() if field not in['payload','source_metadata'])
#     payload = (field for field in data['payload'])
#     source_metadata = (field for field in data['source_metadata'])
#     return cdc_dwh_entity_role_properties(**data_fields,payload=payload,source_metadata=source_metadata)
def dwh_run():
    beam_options = PipelineOptions.from_dictionary(config)
    with beam.Pipeline(options=beam_options) as p:
        notifications = (
                p
                | "read gcs noti" >> beam.io.ReadFromText('/home/hkhnhan/Code/dwh-datflow-pubsub-noti/dwh_entity_role_properties.jsonl')
                # | "read gcs noti" >> beam.io.ReadFromPubSub(subscription="projects/pj-bu-dw-data-sbx/subscriptions/gcs_noti_sub").with_output_types(bytes)
                | "to json" >> beam.Map(json.loads)
                | "to convert datatype" >> beam.Map(convert_to_string)
                # | "schema writer2" >> beam.Map(lambda x:x).with_output_types(beamSQL_schema.test_schema)
                | "schema writer1" >> beam.Map(lambda x:cdc_dwh_entity_role_properties(**x))
                | "schema writer2" >> beam.Map(lambda x:x).with_output_types(cdc_dwh_entity_role_properties)
                # | "sql" >> SqlTransform("""SELECT * FROM PCOLLECTION""")
                | beam.Map(print)
            )
if __name__ == '__main__':
    # Run the pipeline.
    logging.getLogger(__name__).setLevel(logging.INFO)
    dwh_run()


