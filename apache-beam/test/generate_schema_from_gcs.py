from google.cloud import storage
import json
from datetime import datetime
import os
import io
import gzip
import pandas as pd
raw_schema={
            "whitelists" : ["read_timestamp","source_metadata","payload"],
            "droplists":['tx_id','lsn'], # drop columns after flattening
        }
dataset = "CMD"
def change_data_type(obj):
    for key, val in obj.items():
        # Check for datetime strings
        if isinstance(val, str):
            try:
                datetime_obj=datetime.fromisoformat(val)
                obj[key] = datetime_obj
            except ValueError:
                if val.isdigit():
                    obj[key] = int(val)
                elif val.lower() in ('true', 'false'):
                    if obj[key] ==(val.lower() == 'true'):
                        obj[key]=True
                    else: obj[key]=False
                elif isinstance(val, str):
                    try:
                        float_obj = float(val)
                        obj[key] = float_obj
                    except ValueError:
                        pass
        elif isinstance(val, bool):
            pass 
        elif isinstance(val, int):
            pass
    return obj
def flatten_data(element):
    _flat ={}
    _remove=[]
    filtered_dict = {key: value for key, value in element.items() if key in raw_schema.get("whitelists")}
    for k,v in filtered_dict.items():
        if isinstance(v, dict):
            _flat.update({key:value for key,value in v.items()})
            _remove.append(k)
    filtered_dict.update(_flat)
    _remove = _remove+raw_schema.get("droplists")
    for r in _remove:
        filtered_dict.pop(r)
    current_time = datetime.utcnow()
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
    filtered_dict.update({'process_timestamp':formatted_time})
    # change datatype primary keys:
    filtered_dict['primary_keys'] = str(filtered_dict.get('primary_keys'))
    return filtered_dict
def generate_BQ_schema_from_json(element, table_name):
    schema = {}
    fields = []
    for column, val in element.items():
        mode = "NULLABLE"
        type = "STRING"
        if isinstance(val, str):
            type = "STRING"
        elif isinstance(val, float):
            type = "FLOAT"
        elif isinstance(val, bool):
            type = "BOOLEAN" 
        elif isinstance(val, int):
            type = "INTEGER" 
        elif isinstance(val, datetime):
            type = "TIMESTAMP"
        fields.append(
            {
                "name":column,
                "type":type,
                "mode":mode,
                # fields=fields,
            })
        schema['fields'] = fields
    current_dir = os.path.abspath(__file__)
    json_file_path = os.path.join(os.path.abspath(os.path.join(current_dir, os.pardir)), f'datalake/schema/{dataset}', f'{table_name}.json')
    with open( json_file_path, "w",) as fw:
                json.dump(schema, fw, indent=2, sort_keys=True)
                fw.write("\n")
                fw.close()
    return schema

project_id = "pj-bu-dw-data-sbx"
bucket_name = "test_bucket_upvn"
parent_folder_name = "datastream-postgres/datastream/cmd_test"

client = storage.Client(project=project_id)
bucket = client.bucket(bucket_name)
checked_table = []
format_name = f"{parent_folder_name}/*/*/*/*/*/*"
for blob in client.list_blobs(bucket_name, prefix=parent_folder_name):
    file = blob.name.split("/")
    find=file[-1].split('.')
    if find[-1] in ('gz','json','jsonl'):
        # print("blob: " + blob.name)
        blob = bucket.get_blob(blob.name)
        data = io.BytesIO(blob.download_as_string())     
        with gzip.open(data, 'rb') as gz:
        #     # Read compressed file as a file object
            file = gz.read().decode('utf-8')
        # Decode the byte type into string by utf-8
            # s = io.StringIO(blob_decompress)
        json_objects = file.strip().split('\n')
        # print(json_objects[0])
        obj = json_objects[0]
        obj = json.loads(obj)
        if obj['source_metadata']['table'] not in checked_table:
            print("--------------------------------")
            print(obj['source_metadata']['table'])
            # data=decoded_string['payload']
            # print(data)
            generate_BQ_schema_from_json(change_data_type(flatten_data(obj)),obj['source_metadata']['table'])
            checked_table.append(obj['source_metadata']['table'])
        else:
            continue
            

