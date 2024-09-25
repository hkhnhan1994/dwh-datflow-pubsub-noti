"""Configuration of the dataflow."""

beam_config={
    "job_name": "cmd-stream",
    "staging_location": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/staging",
    "temp_location": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/temp",
    "project": "pj-bu-dw-data-sbx",
    "region": "europe-west1",
    "max_num_workers": 20,
    "max_cache_memory_usage_mb": 8096,
    # "number_of_worker_harness_threads": 4,
    "experiments":["no_use_multiple_sdk_containers","enable_data_sampling"],
    "num_workers": 1,
    "worker_region": 'europe-west1',
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
    "project": "pj-bu-dw-data-sbx",
    "subscription": ["test1mess","test_sub","upg-documents-sub"],  # test1mess test_sub gs_noti_dead_letter_sub
    "blob_name_prefix": "datastream-postgres/datastream/cmd_test",
    "bucket_name": "test_bucket_upvn",
    "topic_name": "gcs_noti"
}
cdc_ignore_fields = [
    'stream_name',
    'schema_key',
    'sort_keys',
    'source_metadata.tx_id',
    'source_metadata.lsn',
]
bigquery_datalake ={
    "project": "pj-bu-dw-data-sbx",
    "region": "europe-west1",
    "dataset": {
        "cmd_test":"dev_lake_view_cmd",
        "paci_test":"dev_lake_view_paci",
        "upg-data-sbx-eu-datastream-documents": "dev_btx_doc"
        },
    "default_dataset": "unmap_datalake"
    
}
dead_letter = {
"bq_channel":
    {
        "table_id": "error_log_table",
        "project": "pj-bu-dw-data-sbx",
        "dataset": "lake_view_cmd",
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
        "project": "pj-bu-dw-data-sbx",
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
    