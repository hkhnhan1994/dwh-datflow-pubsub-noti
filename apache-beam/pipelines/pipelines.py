import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, WorkerOptions, StandardOptions
# from apache_beam.runners.interactive.display import pipeline_graph
import logging
from .functions import getSub, convert_nametuple, get_table_model, get_project_mapping, read_dwh_config, read_yaml_datalake_config, load_to_BQ
from apache_beam import coders
from .datalake import transform_datalake, transform_raw_data
from .data_warehouse import transformation

print = logging.info
def apply_option(options,config):
    options.from_dictionary(options.get_all_options())
    options.view_as(GoogleCloudOptions).job_name=config["JOB_NAME"]
    options.view_as(GoogleCloudOptions).staging_location=config["STAGING_LOCATION"]
    options.view_as(GoogleCloudOptions).temp_location=config["TEMP_LOCATION"]
    options.view_as(GoogleCloudOptions).project=config["PROJECT"]
    options.view_as(GoogleCloudOptions).region=config["REGION"]
    options.view_as(WorkerOptions).num_workers = config["NUM_WORKERS"]
    options.view_as(WorkerOptions).max_num_workers = config["MAX_NUM_WORKERS"]
    options.view_as(WorkerOptions).worker_region = config["WORKER_REGION"]
    options.view_as(WorkerOptions).machine_type = config["MACHINE_TYPE"]
    options.view_as(WorkerOptions).disk_size_gb = config["DISK_SIZE_GB"]
    options.view_as(StandardOptions).runner=config["RUNNER"]
    options.view_as(StandardOptions).streaming=True
    return options
def get_pcollections_tables(domain,whitelist_tables):
    schemas={}
    for table in whitelist_tables:
        schema=get_table_model(domain,table['NAME'])
        nametuple_schema=convert_nametuple(schema, table['NAME'])
        coders.registry.register_coder(nametuple_schema, coders.RowCoder)
        schemas[table['NAME']] ={
            "bq_schema": schema,
            "pc_schema": nametuple_schema
        }
    return schemas

def run(options, domain, env):
    mapping_domain=get_project_mapping(domain)
    datalake_config = read_yaml_datalake_config(mapping_domain,env)
    if datalake_config["PIPELINE_ENABLE"]:
        datalake_tables ={}
        if datalake_config["SUBSCRIBER"]:
            _,_getsub= getSub(datalake_config["PROJECT"],datalake_config["SUBSCRIBER"])
            if _getsub:
                print("sub {}".format(_getsub))
                p= beam.Pipeline(options=apply_option(options,datalake_config))
            # Your pipeline logic goes here 
                flatten_data_from_gcs = (p|"datalake of {}".format(datalake_config['JOB_NAME'])
                                            >> transform_raw_data(_getsub, datalake_config)
                                        )
            # parsing and write new records into datalake BQ, having windowtime here
                schemas=get_pcollections_tables(mapping_domain, datalake_config['BQ_DATALAKE_CONFIG']['WHITELIST_TABLES'])
                for table in datalake_config['BQ_DATALAKE_CONFIG']['WHITELIST_TABLES']:
                    # schema= get_table_model(mapping_domain,table['NAME'])
                    classify_datalake = (
                        flatten_data_from_gcs| f"classify {table['NAME']}" 
                            >> transform_datalake(table,schemas[table['NAME']]['bq_schema'],datalake_config)
                        )
                #     #Prepare for transformations
                #     print(schemas[table['NAME']]["pc_schema"])
                    datalake_tables.update({table['NAME']:
                                            (
                                                classify_datalake
                                                |f"mapping schema: {table['NAME']}" 
                                                    # >> beam.Map(lambda x: schemas[table['NAME']]["pc_schema"](**x)).with_output_types(schemas[table['NAME']]["pc_schema"])
                                                    >> beam.Map(lambda x:x).with_output_types(schemas[table['NAME']]["pc_schema"])
                                                    # >> beam.Map(lambda x: schemas[table['NAME']]["pc_schema"](**x)).with_output_types(typing.Iterable[bytes])
                                            )})
                # Performing the transformation
                trans_tables, pcollection_tables = read_dwh_config(mapping_domain, env )
                transformations ={}
                for dwh_table, trans_info in trans_tables.items():
                    list_enities= {}
                    for entity in pcollection_tables:
                        list_enities.update({entity: datalake_tables[entity]})

                        
                    # print("{}:{}".format(dwh_table,list_enities))
                        # print("{}:{}".format(dwh_table,trans_info))
                    transformations.update({ dwh_table:list_enities  | transformation(trans_info)})
            
            # apply kms
            # print(pipeline_graph.PipelineGraph(p).get_dot())
            result = p.run()
            result.wait_until_finish()