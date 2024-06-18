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
    "runner": "DirectRunner", # DirectRunner DataflowRunner
    "setup_file": './setup.py',
    "save_main_session" : True,
    "streaming": True
}
pubsub_config={
    "project": "pj-bu-dw-data-sbx",
    "subscription": "test_sub"
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
    "data_set": "lake_view_cmd",
}
pipeline_config ={
    "CMD":{
        "datalake":{
            "WHITELISTED_TABLES":[
                'cdc_metadata', # for cdc datastream metadata
                'source_metadata',# for cdc datastream metadata
                'dwh_entity_role_properties',
                ],
        }
    },
}
