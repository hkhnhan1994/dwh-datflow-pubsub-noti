# from typing import Any, List, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from apache_beam.io import ReadAllFromTextContinuously
from apache_beam.runners.interactive.display import pipeline_graph
import logging
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
from .functions import getTopic, read_yaml_pubsub_config

print = logging.info

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, 
    data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback
class publish_topic(beam.DoFn):
    def __init__(self, config):
        self.config=config
    def process(self, element):
        _ref_topic,_exist_topic = getTopic(element,self.config['PROJECT'],self.config['DATABASES'],aditional_prefix = "gcs")
        if _exist_topic:
            publisher = pubsub_v1.PublisherClient()
            # print("found topic {} ".format(_exist_topic.name))
            publish_future = publisher.publish(_exist_topic.name, data=element)
            publish_future.add_done_callback(get_callback(publish_future, element))
            # result=future.result()
        else:
            print("topic {} does not exist".format(_ref_topic))

def run(options, env):
    # get_projects=get_project_mapping()
    pubsub_config_info = read_yaml_pubsub_config(env)
    options.view_as(GoogleCloudOptions).staging_location=pubsub_config_info["STAGING_LOCATION"]
    options.view_as(GoogleCloudOptions).temp_location=pubsub_config_info["TEMP_LOCATION"]
    options.view_as(GoogleCloudOptions).project=pubsub_config_info["PROJECT"]
    options.view_as(GoogleCloudOptions).region=pubsub_config_info["REGION"]
    options.view_as(WorkerOptions).num_workers = pubsub_config_info["NUM_WORKERS"]

    options.view_as(WorkerOptions).max_num_workers = pubsub_config_info["MAX_NUM_WORKERS"]
    options.view_as(WorkerOptions).worker_region = pubsub_config_info["WORKER_REGION"]
    options.view_as(WorkerOptions).machine_type = pubsub_config_info["MACHINE_TYPE"]
    options.view_as(WorkerOptions).disk_size_gb = pubsub_config_info["DISK_SIZE_GB"]
    
    # options.view_as(StandardOptions).runner="DirectRunner"
    options.view_as(StandardOptions).runner=pubsub_config_info["RUNNER"]
    # options.view_as(StandardOptions).streaming=True
    
    # options.from_dictionary(options.get_all_options())
    options.view_as(GoogleCloudOptions).job_name=pubsub_config_info["JOB_NAME"]
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(SetupOptions).setup_file= './setup.py'
    # Create the pipeline.
    pipeline = beam.Pipeline(options=options)

    # Steps:
    # 1) Read from the text source.
    # 2) Write each text record to Pub/Sub
    with pipeline:
        read_from_gcp = (
            pipeline
             | "GCS read" >> ReadAllFromTextContinuously(pubsub_config_info["FILE_PATH"])  
        )
        encoding = (
            read_from_gcp
            | "encoding bytestring" >> beam.Map(lambda x: x.encode("utf-8")).with_output_types(bytes)  
        )
        pubsub = (
            encoding
            | "Assign to Topic" >> beam.ParDo(publish_topic(pubsub_config_info)) 
        )
        print(pipeline_graph.PipelineGraph(pipeline).get_dot())
        result = pipeline.run()
        result.wait_until_finish()
