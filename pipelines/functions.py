import apache_beam as beam
import json
import io
from apache_beam.io.gcp.bigquery_tools import  get_bq_tableschema, BigQueryWrapper
from google.cloud import bigquery
import datetime
import logging
print = logging.info

class merge_schema(beam.DoFn):
    def process(self, merge_schema):
        # Convert lists to dictionaries with "name" as the key
        _cur_schema = merge_schema[1]['bq_schema']
        _ex_schema = merge_schema[1]['avro_schema']
        dict_cur_schema = {item['name']: item for item in _cur_schema}
        dict_ex_schema = {item['name']: item for item in _ex_schema}
        # Merge dictionaries
        merged_dic_schema = {**dict_cur_schema, **dict_ex_schema}
        # Convert merged dictionary back to a list
        merged_schema = list(merged_dic_schema.values())
        # Identify items in _ex_schema that are not in _cur_schema   
        diff_list = [item for item in _ex_schema if item['name'] not in dict_cur_schema]
        # Identify items in _cur_schema that are not in _ex_schema   
        diff_list_but_no_update_schema = [item for item in _ex_schema if item['name'] not in dict_cur_schema]
        if bool(diff_list):
            print(f'found different between current schema and exists schema:{diff_list}')
        if bool(diff_list_but_no_update_schema):
            print(f'found different between current schema and exists schema but no update:{diff_list_but_no_update_schema}')
        is_new_table =False if len(_cur_schema) >0 else True
        yield (merge_schema[0],{'schema':merged_schema, 'is_schema_changes':bool(diff_list),'is_new_table':is_new_table })       
class read_bq_schema(beam.DoFn):
    
    def __init__(self,project ,dataset):
        self.project=project
        self.dataset=dataset
        # self.client
    def setup(self):
        self.client = bigquery.Client()
    def process(self,schema):
        try:
            dataset_ref = self.client.dataset(dataset_id=self.dataset, project=self.project)
            table_ref = dataset_ref.table(schema[0])
            table = self.client.get_table(table_ref)
            f = io.StringIO("")
            self.client.schema_to_json(table.schema, f)
            bq_table = json.loads(f.getvalue())
            yield (schema[0],{"bq_schema":bq_table, "avro_schema":schema[1]})
        except:
            print("not found table {}".format(schema[0]))
            yield (schema[0],{"bq_schema":[], "avro_schema":schema[1]})
class create_table(beam.DoFn):
    def __init__(self,project,dataset):
        self.project=project
        self.dataset=dataset
    def setup(self):
        self.bq_table = BigQueryWrapper()
    def _parse_schema_field(self,field):
        return bigquery.SchemaField(
            name = field['name'],
            field_type = field['type'],
            mode = field['mode'] if 'mode' in field else 'NULLABLE',       
            fields = [self._parse_schema_field(x) for x in field['fields']] if 'fields' in field else '',
            description= field['description'] if 'description' in field else '',
            )
    def process(self,data):
        if data[1]['is_new_table']: 
            # if new table detected
            schema = get_bq_tableschema({'fields':data[1]['schema']})
            self.bq_table.get_or_create_table(
                project_id = self.project,
                dataset_id = self.dataset,
                table_id = data[0],
                schema = schema,
                create_disposition= 'CREATE_IF_NEEDED',
                write_disposition= 'WRITE_EMPTY'
            )
            print(f'new table {data[0]} has been created')
        else:
            if data[1]['is_schema_changes']: 
                # if schema changes detected
                # print('schema changes detected')
                schema = [self._parse_schema_field(f) for f in data[1]['schema']]
                # schema = get_bq_tableschema({'fields':data[1]['schema']})
                table = self.bq_table.gcp_bq_client.get_table(
                    f'{ self.project}.{self.dataset}.{data[0]}'
                )
                table.schema = schema
                self.bq_table.gcp_bq_client.update_table(table, ['schema'])

                print('schema changes updated')
        yield (data[0], data[1]['schema'])
class enrich_data(beam.DoFn):
    def process(self, data):
        schema= data[1]['bq_schema'][0]
        list_of_data= []
        for dt in data[1]['data']:
            fill_null = {}
            for field in schema:
                field_name = field['name']
                fill_null[field_name] = dt.get(field_name, None)
            list_of_data.append(fill_null)
        data[1]['data'] = list_of_data
        data[1]['bq_schema'] = schema
        # data[1]['bq_schema'][0] = data[1]['bq_schema']['schema']
        yield (data[0],data[1])
def write_dead_letter(data):
    pass