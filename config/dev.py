beam_config={
    "job_name": "cmd-stream",
    "staging_location": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/staging",
    "temp_location": "gs://test_bucket_upvn/datastream-postgres/Beam-pubsub/temp",
    "project": "pj-bu-dw-data-sbx",
    "region": "europe-west1",
    "max_num_workers": 1,
    "num_workers": 1,
    "worker_region": 'europe-west1',
    "machine_type": 'n1-standard-1',
    "disk_size_gb": 10,
    "runner": "DirectRunner", # DirectRunner DataflowRunner
    "setup_file": './setup.py',
    "save_main_session" : True,
    "streaming":True
}

gcs_datalake ={
    "pucket": "test_bucket_upvn",
    "sub_folder": "datastream-postgres/datastream",
    "project": "pj-bu-dw-data-sbx",
    "region": "europe-west1",
}

WHITELISTED_TABLES = [
    "dwh_entity_role_properties"
]