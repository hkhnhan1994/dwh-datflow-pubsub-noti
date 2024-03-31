from apache_beam.options.pipeline_options import SetupOptions, GoogleCloudOptions, WorkerOptions, StandardOptions
import logging
import apache_beam as beam
from apache_beam import coders
from datalake  import beamSQL_schema
from apache_beam.transforms.sql import SqlTransform
import json
# print = logging.info
config ={
    "JOB_NAME": "cmd-stream-1",
    "PIPELINE_ENABLE": True,
    "SUBSCRIBER": "gcs_cmd_stream_test-sub",
    "STAGING_LOCATION": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/staging",
    "TEMP_LOCATION": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/temp",
    "PROJECT": "pj-bu-dw-data-sbx",
    "REGION": "europe-west1",
    "MAX_NUM_WORKERS": 1,
    "NUM_WORKERS": 1,
    "WORKER_REGION": 'europe-west1',
    "MACHINE_TYPE": 'n1-standard-1',
    "DISK_SIZE_GB": 10,
    "RUNNER": "DataflowRunner", # DirectRunner DataflowRunner
}
def apply_option(options,config):
    options.from_dictionary(options.get_all_options())
    options.view_as(GoogleCloudOptions).job_name=config["JOB_NAME"]
    options.view_as(GoogleCloudOptions).staging_location=config["STAGING_LOCATION"]
    options.view_as(GoogleCloudOptions).temp_location=config["TEMP_LOCATION"]
    options.view_as(GoogleCloudOptions).project=config["PROJECT"]
    options.view_as(GoogleCloudOptions).region=config["REGION"]
    options.view_as(WorkerOptions).num_workers = config["NUM_WORKERS"]
    options.view_as(WorkerOptions).max_num_workers = config["MAX_NUM_WORKERS"]
    options.view_as(WorkerOptions).worker_region = config["WORKER_REGION"]
    options.view_as(WorkerOptions).machine_type = config["MACHINE_TYPE"]
    options.view_as(WorkerOptions).disk_size_gb = config["DISK_SIZE_GB"]
    options.view_as(StandardOptions).runner=config["RUNNER"]
    options.view_as(StandardOptions).streaming=True
    return options

def is_file(mess):
    return mess['name'] == ('%.jsonl' or '%.jsonl.gz')
def dwh_run(options):
    p= beam.Pipeline(options=apply_option(options,config))
    notifications = (
            p
             | "read gcs noti" >> beam.io.ReadFromPubSub(subscription="projects/pj-bu-dw-data-sbx/subscriptions/gcs_noti_sub").with_output_types(bytes)
            #  | "decoding" >> beam.Map(lambda x:x.decode('utf-8'))
             | "to json" >> beam.Map(json.loads)
             | "schema writer" >> beam.Map(lambda x: beamSQL_schema.gcs_notification_message(**x)).with_output_types(beamSQL_schema.gcs_notification_message)
            | "query the name" >> beam.Filter(lambda mess: '.jsonl' or '.jsonl.gz'  in mess.name.lower())
            
            #  | "query the name" >> SqlTransform(dialect='zetasql',query="SELECT name FROM PCOLLECTION")
            | beam.Map(print)
        )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    options = GoogleCloudOptions()
    options.view_as(SetupOptions).save_main_session = True  
    options.view_as(SetupOptions).setup_file= './setup.py'
    # Run the pipeline.
    logging.getLogger(__name__).setLevel(logging.INFO)
    dwh_run(options)


