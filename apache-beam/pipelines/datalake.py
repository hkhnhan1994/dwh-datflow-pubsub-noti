
from apache_beam.io import ReadFromPubSub
import logging
import apache_beam as beam
from apache_beam import window
from .functions import process_cdc_data, check_membership_and_update, convert_nametuple
from .functions import load_to_BQ
from apache_beam import coders
print = logging.info

class transform_raw_data(beam.PTransform):
    def __init__(self, subscription_path, datalake_info):
        self.subscription_path=subscription_path.name
        self.datalake_info=datalake_info
    def expand(self, pcoll):
        def fill_data(element):
            #if cdc data:
            return process_cdc_data(element,self.datalake_info['CDC']['RAW_SCHEMA']['DROPLISTS'],self.datalake_info['CDC']['RAW_SCHEMA']['WHITELISTS'])
        return  (
            pcoll
            |f"Read from sub" >> ReadFromPubSub(subscription=self.subscription_path)
            |f"transform {self.subscription_path}" >> beam.Map(fill_data)
        )
        # to_dataframe  = convert.to_dataframe(data)

class transform_datalake(beam.PTransform):
    def __init__(self,table_config,schema,datalake_config):
        self.table_config= table_config
        self.schema = schema
        self.datalake_config = datalake_config
        self.schema_schanges =None
    def expand(self, pcoll):
        def filter(element):
            # print("getting data for table {}".format(element['table']))
            # print(element)
            return element['table'] == self.table_config['NAME']
        def schema_check(element):
            defined_fields = [field['name'] for field in self.schema['fields']]
            current_fields = element.keys()
            miss_fields, extra_fields = check_membership_and_update(defined_fields,current_fields)
            if miss_fields:
                print(f"schema changes on table {self.table_config['NAME']} fields are missing: {miss_fields}")
            if extra_fields:
                print(f"schema changes on table {self.table_config['NAME']} fields are extra: {extra_fields}")
            # update schema changes:
            for extra_field in extra_fields:
                self.schema_schanges['fields'].update({ 
                                                "mode": "NULLABLE",
                                                "name": extra_field,
                                                "type": "STRING"
                                            })
            for miss_field in miss_fields:
                element[miss_field] = "NA"       
            return element
        def get_defined_data(element):
            defined_fields = [field['name'] for field in self.schema['fields']]
            # print({key: value for key, value in element.items() if key in defined_fields})
            return {key: element[key] for key in defined_fields if key in element}
        filtering = (pcoll
            |f"classifying {self.table_config['NAME']}" >> beam.Filter(filter)
            # |f"schema checking {self.table_config['NAME']}" >> beam.Map(schema_check)
            )
        windowing =(
            filtering
            |f"windowing {self.datalake_config['WINDOWING_TIME']} table {self.table_config['NAME']} " >> beam.WindowInto(window.FixedWindows(self.datalake_config['WINDOWING_TIME']))
        )
        to_BQ = (
            windowing
            |f"schema checking {self.table_config['NAME']}" >> beam.Map(schema_check)
            |f"GlobalWindows {self.table_config['NAME']}" >>  beam.WindowInto(beam.window.GlobalWindows())
            |f"to BQ {self.table_config['NAME']}" >> load_to_BQ({
                            "PROJECT": self.datalake_config['BQ_DATALAKE_CONFIG']['PROJECT'],
                            "DATASET":self.datalake_config['BQ_DATALAKE_CONFIG']['DATASET'],
                            "TABLE": self.table_config['NAME'],
                            "QUERY":None,
                            "PCOLLECTION_TABLES" : None,
                            "SCHEMA":self.schema_schanges,
                            "ADDITIONAL_BQ_PARAMETERS":self.table_config['ADDITIONAL_INFO'],
                            'WRITE_DISPOSITION':"WRITE_APPEND",
                            'CREATE_DISPOSITION': "CREATE_IF_NEEDED",
                        })
        )
        defined_data = (
            windowing
            |f"get defined columns in {self.table_config['NAME']}" >> beam.Map(get_defined_data)
        )
        return defined_data