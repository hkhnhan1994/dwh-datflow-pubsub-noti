from apache_beam import coders
from typing import NamedTuple, Sequence, Optional
from apache_beam.utils.timestamp import Timestamp
from file import get_schema_datalake
from config.develop import pipeline_config
# CDC metadata schema
def create_nametuple_schema(*schemas,table_name):
    class_attributes = []
    mapping ={
        "str":str,
        "int":int,
        "bool":bool,
        "float":float,
        # "Numeric",
        "List":Sequence,
        "Timestamp":Timestamp
    }
    for sche in schemas:
        for name,type in sche.items():
            class_attributes.append((name,Optional[mapping[type]]))
    return NamedTuple(table_name,class_attributes)

def generate_nametuple_schema(whitelist):
    tables_schema={}
    for name,schema in whitelist.items():
        if name not in['cdc_metadata','source_metadata']:
            try:
                tables_schema[name]=create_nametuple_schema(whitelist.get('cdc_metadata',None),whitelist.get('source_metadata',None),schema,table_name=name)
            except:
                print("not a cdc message")
                tables_schema[name]=create_nametuple_schema(schema,table_name=name)
    return tables_schema

def register_beam_coder(name_class):
    return coders.registry.register_coder(name_class, coders.RowCoder)

def read_datalake_schema():
    for domain_name,config in pipeline_config.items():
        domain_schemas={}
        table_schemas = {}
        datalake_whitelist= config.get('datalake').get('WHITELISTED_TABLES')
        for table_name in datalake_whitelist:
            table_schemas[table_name]=get_schema_datalake(domain_name,table_name)
        domain_schemas[domain_name]= generate_nametuple_schema(table_schemas)
        # if it is a cdc stream:
    return domain_schemas

# new_schema=read_datalake_schema()
# for i,val in new_schema.items():
#     print(f"{i}:{val}")