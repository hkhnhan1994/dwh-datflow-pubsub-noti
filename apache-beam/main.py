from apache_beam.options.pipeline_options import SetupOptions, GoogleCloudOptions
import logging
from pipelines.pipelines import run as dwh_run
from pipelines.text_to_pubsub import run as pubsub_dataflow_run

print = logging.info
if __name__ == '__main__':
    options = GoogleCloudOptions()
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(SetupOptions).setup_file= './setup.py'
    # Run the pipeline.
    logging.getLogger(__name__).setLevel(logging.INFO)
    dwh_run(options, "cmd-stream-test", 'dev')

    # pubsub_dataflow_run(options, "dev")