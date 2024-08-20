"""Configuration of the dataflow."""

beam_config={
    "job_name": "cmd-stream",
    "staging_location": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/staging",
    "temp_location": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/temp",
    "project": "pj-bu-dw-data-sbx",
    "region": "europe-west1",
    "max_num_workers": 3,
    "num_workers": 1,
    "worker_region": 'europe-west1',
    "machine_type": 'n1-standard-1',
    "disk_size_gb": 10,
    "runner": "DataflowRunner", # DirectRunner DataflowRunner
    "setup_file": './setup.py',
    "save_main_session" : True,
    "streaming": True
}
pubsub_config={
    "project": "pj-bu-dw-data-sbx",
    "subscription": ["test1mess","test_sub"]  # test1mess test_sub gs_noti_dead_letter_sub
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
        "cmd_test":"lake_view_cmd",
        "paci_test":"lake_view_paci"
        }
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
else:
    import logging
    print_info = logging.info
    print_debug = logging.debug
    print_error = logging.error
    