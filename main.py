from apache_beam.options.pipeline_options import PipelineOptions
import logging
from config.develop import beam_config
from pipelines.pipeline import run

from apache_beam.options.pipeline_options import SetupOptions, GoogleCloudOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions, WorkerOptions, StandardOptions

if __name__ == '__main__':
    # Run the pipeline.
    logging.getLogger(__name__).setLevel(logging.INFO)
    # options = GoogleCloudOptions()
    # options.view_as(SetupOptions).save_main_session = True
    # options.view_as(SetupOptions).setup_file= './setup.py'
    # # options.from_dictionary(options.get_all_options())
    # options.view_as(GoogleCloudOptions).job_name="cmd-stream-1"
    # options.view_as(GoogleCloudOptions).staging_location="gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/staging"
    # options.view_as(GoogleCloudOptions).temp_location="gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/temp"
    # options.view_as(GoogleCloudOptions).project="pj-bu-dw-data-sbx"
    # options.view_as(GoogleCloudOptions).region="europe-west1"
    # options.view_as(WorkerOptions).num_workers = 1
    # options.view_as(WorkerOptions).max_num_workers = 1
    # options.view_as(WorkerOptions).worker_region = 'europe-west1'
    # options.view_as(WorkerOptions).machine_type = 'n1-standard-1'
    # options.view_as(WorkerOptions).disk_size_gb = 10
    # options.view_as(StandardOptions).runner='DirectRunner'
    # options.view_as(StandardOptions).streaming = True
    beam_options = PipelineOptions.from_dictionary(beam_config)
    run(beam_options)