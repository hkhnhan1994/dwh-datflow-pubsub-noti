
import yaml
import re
# from apache_beam.utils.timestamp import Timestamp
# from apache_beam.typehints.schemas import LogicalType, MillisInstant
import datetime
import os
import json
import apache_beam as beam
import logging

class load_to_BQ(beam.PTransform):
    def __init__(self, BQ_info:dict={}):
        self.BQ_info=BQ_info
    def expand(self,pcoll):
        # print(self.BQ_info)
        schema = self.BQ_info.get('SCHEMA')
        write_disposition = self.BQ_info.get('WRITE_DISPOSITION', 'WRITE_APPEND')
        create_disposition = self.BQ_info.get('CREATE_DISPOSITION', 'CREATE_IF_NEEDED')
        additional_bq_parameters = self.BQ_info.get('ADDITIONAL_BQ_PARAMETERS',None)
        return (
            pcoll
            |f"Write to BQ table {self.BQ_info['PROJECT']}.{self.BQ_info['DATASET']}.{self.BQ_info['TABLE']}" >> beam.io.WriteToBigQuery(
                f"{self.BQ_info['PROJECT']}:{self.BQ_info['DATASET']}.{self.BQ_info['TABLE']}",
                schema=schema,
                write_disposition=write_disposition,
                create_disposition=create_disposition,
                insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
                ignore_unknown_columns=True,
                additional_bq_parameters=additional_bq_parameters,
                with_auto_sharding=True,
                # kms_key= self.Option['CREATE_DISPOSITION'],
                # method='STREAMING_INSERTS',
            )
        ) 
def process_cdc_data(element, droplists,whitelists):
            obj = json.loads(element)
            _flat ={}
            _remove=[]
            filtered_dict = {key: value for key, value in obj.items() if key in whitelists}
            for k,v in filtered_dict.items():
                if isinstance(v, dict):
                    _flat.update({key:value for key,value in v.items()})
                    _remove.append(k)
            filtered_dict.update(_flat)
            _remove = _remove+ droplists
            for r in _remove:
                filtered_dict.pop(r)
            current_time = datetime.datetime.utcnow()
            formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
            filtered_dict.update({'process_timestamp':formatted_time})
            # change datatype primary keys:
            filtered_dict['primary_keys'] = str(filtered_dict.get('primary_keys'))
            #get_project
            print("{} flattened".format(filtered_dict['table']))
            # return json.dumps(filtered_dict,ensure_ascii=False) #.encode('utf8')
            return filtered_dict
def get_table_model(dataset: str, table: str) -> dict:
    """Get the table model.

    :return: Table model. Return a dictionary containing a definition of a table
    :rtype: dict
    """
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(os.path.abspath(os.path.join(current_dir, os.pardir)), f'datalake/{dataset}/schema', f'{table}.json')
    with open(
        file_path
    ) as f:
        d = json.load(f)
    return d
def get_project_mapping(project_name: str =None) -> str:
    """Get the table model.

    :return: Table model. Return a dictionary containing a definition of a table
    :rtype: dict
    """
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(os.path.abspath(os.path.join(current_dir, os.pardir)), 'datalake', 'mapping.json')
    with open(file_path, 'r') as f:
        d = dict(json.load(f))
    if project_name:
        return d.get(project_name)
    else: return d
def extract_substring(input_string):
    """Extract table name from input string."""
    # Define the regular expression pattern
    pattern = re.compile(r"(?:from|join)\s*(\S+)", re.IGNORECASE)

    # Search for the pattern in the input string
    matches = pattern.findall(input_string)

    # If a match is found, extract the substring
    if matches:
        return matches
    else:
        return None
def mappinng_table(tables: list):
    result = {}
    internal_tables =[]
    for table in tables:
        try:
            tb = table.split(".")
            project = tb[0]
            table_name = tb[1]
            if project.lower() == 'stream':
                result[table] = table_name
                internal_tables.append(table_name)
        except:
            result[table] = table
            continue
    return result, internal_tables
def read_yaml_transformation(file_path):
    '''Read transformation for file '''
    with open(file_path, "r") as f:
        trans = yaml.safe_load(f)
    query = trans["QUERY"]
    rename_tables, pcollection_tables = mappinng_table(extract_substring(query))
    
    for table, renamed in rename_tables.items():
        query = query.replace(table, renamed)
    additional_bq_parameters = {
        "timePartitioning": trans['TIMEPARTITIONING'],
        "clustering": trans['CLUSTER_FIELDS'],
    }
    return {
        "QUERY":query,
        "SCHEMA":{"fields":trans["SCHEMA"]},
        "ADDITIONAL_BQ_PARAMETERS":additional_bq_parameters,
        'WRITE_DISPOSITION':trans["WRITE_DISPOSITION"],
        'CREATE_DISPOSITION': trans["CREATE_DISPOSITION"],
    }, pcollection_tables
def read_dwh_config(domain, env):
    '''Get list of transformations.'''
    trans_info = {}
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(os.path.abspath(os.path.join(current_dir, os.pardir)), f'data_warehouse/{domain}/config_{env}.yml')
    with open(file_path, "r") as f:
        dwh_config = yaml.safe_load(f)
        # print(config)
    for tb in dwh_config['WHITELISTS']:
        trans_file_path = os.path.join(os.path.abspath(os.path.join(current_dir, os.pardir)), f'data_warehouse/{domain}/queries/{tb}.yml')
        trans_info[tb], pcollection_tables = read_yaml_transformation(trans_file_path)
        trans_info[tb].update({"PROJECT":dwh_config['PROJECT']})
        trans_info[tb].update({"DATASET":dwh_config['DATASET']})
        trans_info[tb].update({"TABLE":tb})
    return trans_info, pcollection_tables
def read_yaml_datalake_config(domain, env):
    '''Read config.'''
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(os.path.abspath(os.path.join(current_dir, os.pardir)), f'datalake/{domain}/config_{env}.yml')
    with open(file_path, "r") as f:
        return yaml.safe_load(f)   
def read_yaml_pubsub_config(env):
    '''Read config.'''
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(os.path.abspath(os.path.join(current_dir, os.pardir)), f'pubsub/config_{env}.yml')
    with open(file_path, "r") as f:
        return yaml.safe_load(f)
def check_membership_and_update(source_list, current_list):
  missing_members = set(source_list) - set(current_list)
  extra_members = set(current_list) - set(source_list)
  return missing_members, extra_members



######## New lib

def convert_to_string(data):
    if isinstance(data, list):
        return [convert_to_string(item) for item in data]
    elif isinstance(data, dict):
        return {key: convert_to_string(value) for key, value in data.items()}
    else:
        return str(data)
    