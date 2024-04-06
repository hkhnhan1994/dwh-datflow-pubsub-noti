from apache_beam.options.pipeline_options import PipelineOptions
import logging
from config.dev import beam_config, gcs_datalake
from pipelines.pipeline import run
        
if __name__ == '__main__':
    # Run the pipeline.
    logging.getLogger(__name__).setLevel(logging.INFO)
    beam_options = PipelineOptions.from_dictionary(beam_config)
    run(beam_options,gcs_datalake)