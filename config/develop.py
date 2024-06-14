beam_config={
    "job_name": "cmd-stream-1",
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
