"""Configuration of the dataflow."""

beam_config={
    "job_name": "cmd-stream3",
    "staging_location": "gs://justatestbucket123/datastream-postgres/Beam-pubsub/staging",
    "temp_location": "gs://justatestbucket123/datastream-postgres/Beam-pubsub/temp",
    "project": "halogen-parser-471115-i4",
    "region": "europe-west2",
    "max_num_workers": 2,
    "max_cache_memory_usage_mb": 8096,
    # "number_of_worker_harness_threads": 4,
    "experiments":["no_use_multiple_sdk_containers","enable_data_sampling"],
    "num_workers": 1,
    "worker_region": 'europe-west2',
    "machine_type": 'n2-highmem-2',
    "disk_size_gb": 20,
    "runner": "DataflowRunner", # DirectRunner DataflowRunner
    "setup_file": './setup.py',
    "save_main_session" : True,
    "streaming": True,
    "autoscaling_algorithm": "THROUGHPUT_BASED",
    "prebuild_sdk_container_engine": "cloud_build"

}
pubsub_config={
    "project": "halogen-parser-471115-i4",
    "subscription": ["test_sub-sub"],  # test1mess test_sub gs_noti_dead_letter_sub
    "blob_name_prefix": "datastream-postgres/datastream/datastream-postgres/datastream/",
    "bucket_name": "justatestbucket123",
    "topic_name": "test_sub"
}
cdc_ignore_fields = [
    'stream_name',
    'schema_key',
    'sort_keys',
    'source_metadata.tx_id',
    'source_metadata.lsn',
]
cdc_complex_fields = [ #complex fields have to be converted to bigquery data type
    "source_metadata",
    "payload"
]
bigquery_datalake ={
    "project": "halogen-parser-471115-i4",
    "region": "europe-west1",
    "dataset": {
        "postgres":"test_alloydb",
        },
    "default_dataset": "unmap_datalake",
    "additional_create_parameters":{
                    'timePartitioning': 
                        {
                            'type': 'DAY',
                            'field': 'ingestion_meta_data_processing_timestamp'
                        }
                }
    
}
dead_letter = {
"bq_channel":
    {
        "table_id": "error_log_table",
        "project": "halogen-parser-471115-i4",
        "dataset": "dev_dl_error_log",
        "schema":{'fields': [
                {'name': 'destination', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'row', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'stage', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
                ]},
    },
"chat_channel":
    {
        "topics": "gs_noti_dead_letter",
        "project": "halogen-parser-471115-i4",
    },
        }
LOCAL_LOG = False

if LOCAL_LOG:
    print_info = print
    print_debug = print
    print_error = print
    print_info("local log")
else:
    import logging
    print_info = logging.info
    print_debug = logging.debug
    print_error = logging.error
    