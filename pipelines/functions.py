"""Functions that use in transformations."""

import apache_beam as beam
import json
import io
from apache_beam.io.gcp.bigquery_tools import  get_bq_tableschema, BigQueryWrapper
from google.cloud import bigquery
import datetime
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from avro.datafile import DataFileReader
from avro.io import DatumReader
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import TaggedOutput
import hashlib
from config.develop import print_debug,print_error,print_info

# print = logging.info
class merge_schema(beam.DoFn):
    """Merge current schema into exists schema.
    
    Return:
    
        Result.data -> {
            "schema": # Merged Schema
                [
                    {
                        "name": str,
                        "type": str,
                        "mode": str,
                    },
                ],
            "is_schema_changes": Bool,
            "is_new_table":Bool,
            "datalake_maping":
            {
                "project": str,
                "dataset" : str,
                "table": str,
                "bq_pars": {args},
            }
        }
        Result.error -> dead_letter_message

    """
    def process(self, merge_schema):
        try:
            # Convert lists to dictionaries with "name" as the key
            _cur_schema = merge_schema['bq_schema']
            _ex_schema = merge_schema['avro_schema']['fields']
            # print_debug(_cur_schema)
            # print_debug(_ex_schema)
            dict_cur_schema = {item['name']: item for item in _cur_schema} if len(_cur_schema)>0 else {}
            dict_ex_schema = {item['name']: item for item in _ex_schema}
            # Merge dictionaries
            merged_dic_schema = {**dict_cur_schema, **dict_ex_schema}
            # Convert merged dictionary back to a list
            merged_schema = list(merged_dic_schema.values())
            # Identify items in _ex_schema that are not in _cur_schema   
            diff_list = [item for item in _ex_schema if item['name'] not in dict_cur_schema]
            # Identify items in _cur_schema that are not in _ex_schema   
            diff_list_but_no_update_schema = [item for item in _cur_schema if item['name'] not in dict_ex_schema]
            is_new_table = False if len(_cur_schema) >0 else True
            if is_new_table is False:
                if bool(diff_list):
                    print_info(f'extra fields: {diff_list}')
                else: print_debug('schema of table {}  does not change while checking new fields'.format(merge_schema['datalake_maping']['table']))
                if bool(diff_list_but_no_update_schema):
                    print_info('table {} deleted fields: {}'.format(merge_schema['datalake_maping']['table'],diff_list_but_no_update_schema))
                else:  print_debug('schema of table {} does not change while checking removed fields'.format(merge_schema['datalake_maping']['table']))
            else : 
                print_info('found new table {} in source'.format(merge_schema['datalake_maping']['table']))
            yield ({'schema':merged_schema, 'is_schema_changes':diff_list,'is_new_table':is_new_table,'datalake_maping': merge_schema['datalake_maping']})      
        except Exception as e:
            print_error('data {} --> get error {}'.format(merge_schema,e))
            result = dead_letter_message(
                destination= 'schema_processing', 
                row = merge_schema,
                error_message = e,
                stage='merge_schema'
            )
            yield TaggedOutput('error',result)
class read_bq_schema(beam.DoFn):
    """Read table schema from Bigquery.

    Return: Bigquery schema structure read from Bigquery table:
    
        Result.data -> {
            "bq_schema": 
                [
                    {
                        "name": str,
                        "type": str,
                        "mode": str,
                    },
                ]
            "avro_schema":
            {
                "name": str,
                "type": str,
                "fields: [ # BQ schema format( converted avro schema)
                    {
                        "name": str,
                        "type": str,
                        "mode": str,
                    },
                    
                ],
                "path": str,
            },
            "datalake_maping":
            {
                "project": str,
                "dataset" : str,
                "table": str,
                "bq_pars": {args},
            }
        }
        Result.error -> dead_letter_message

    """
    def setup(self):
        self.client = bigquery.Client()
    def process(self,schema, bq_pars):
        dataset_id  = get_data_maping(schema['path'],bq_pars.get('default_dataset'),bq_pars.get('dataset'))
        datalake_maping ={
                "project": bq_pars.get('project'),
                "dataset" : dataset_id,
                "table": schema['name'],
                "bq_pars":bq_pars
            }
        try:
            print_debug("{} store to {}".format(schema['path'],dataset_id))
            dataset_ref = self.client.dataset(dataset_id=dataset_id, project=datalake_maping['project'])
            table_ref = dataset_ref.table(datalake_maping['table'])
            table = self.client.get_table(table_ref)
            f = io.StringIO("")
            self.client.schema_to_json(table.schema, f)
            bq_table = json.loads(f.getvalue())
            yield ({"bq_schema":bq_table, "avro_schema":schema, "datalake_maping":datalake_maping})
        except Exception as e:
            try:
                # print_error(e)
                print_info("not found table {}.{}.{} on BQ".format(bq_pars.get('project'),dataset_id,schema['name']))
                print_debug({"bq_schema":[], "avro_schema":schema, "datalake_maping":datalake_maping})
                yield ({"bq_schema":[], "avro_schema":schema, "datalake_maping":datalake_maping})
            except Exception as e:
                print_error(e)
                result = dead_letter_message(
                    destination= 'schema_processing', 
                    row = schema,
                    error_message = e,
                    stage='read_bq_schema'
                )
                yield TaggedOutput('error',result)
class create_table(beam.DoFn):
    """Create Bigquery table if not exists.
    
    Return:
    
        Result.data -> {
            "schema": # Merged Schema
                [
                    {
                        "name": str,
                        "type": str,
                        "mode": str,
                    },
                ],
            "is_schema_changes": Bool,
            "is_new_table":Bool,
            "datalake_maping":
            {
                "project": str,
                "dataset" : str,
                "table": str,
                "bq_pars": {args},
            }
        }
        Result.error -> dead_letter_message

    """
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
    def process(self,schema):
        try:
            if schema['is_new_table']: 
                # if new table detected
                bq_schema = get_bq_tableschema({'fields':schema['schema']})
                self.bq_table.get_or_create_dataset(
                    project_id = schema['datalake_maping']['project'],
                    dataset_id = schema['datalake_maping']['dataset'],
                    location = schema['datalake_maping']['bq_pars']['region']
                    )
                self.bq_table.get_or_create_table(
                    project_id = schema['datalake_maping']['project'],
                    dataset_id = schema['datalake_maping']['dataset'],
                    table_id = schema['datalake_maping']['table'],
                    schema = bq_schema,
                    create_disposition= 'CREATE_IF_NEEDED',
                    write_disposition= 'WRITE_EMPTY',
                    additional_create_parameters = schema['datalake_maping']['bq_pars'].get('additional_create_parameters')
                )
                print_info('new table {}.{}.{} has been created'.format(
                    schema['datalake_maping']['project'],
                    schema['datalake_maping']['dataset'],
                    schema['datalake_maping']['table']
                )
                )
            else:
                if len(schema['is_schema_changes']) > 0 : 
                    # if schema changes detected
                    # print('schema changes detected')
                    new_schema = [self._parse_schema_field(f) for f in schema['schema']]
                    # schema = get_bq_tableschema({'fields':schema['schema']})
                    table = self.bq_table.gcp_bq_client.get_table(
                        '{}.{}.{}'.format(schema['datalake_maping']['project'],schema['datalake_maping']['dataset'],schema['datalake_maping']['table'])
                    )
                    table.schema = new_schema
                    self.bq_table.gcp_bq_client.update_table(table, ['schema'])
                    print_info('schema changes updated new fields {} into table {}.{}.{}'.format(schema['is_schema_changes'], 
                                                                                        schema['datalake_maping']['project'],
                                                                                        schema['datalake_maping']['dataset'],
                                                                                        schema['datalake_maping']['table'] 
                                                                                        ))
            # yield (schema['name'], schema['schema'])
            yield schema
        except Exception as e:
            print_error(e)
            result = dead_letter_message(
                destination= 'schema_processing', 
                row = schema,
                error_message = e,
                stage='create_table'
            )
            yield TaggedOutput('error',result)
class fill_null_data(beam.DoFn):
    """Filled the missing columns when schema changes happened."""
    def process(self, data):
        try:
            if len(data['bq_schema']['schema']) >0:
                schema= data['bq_schema']['schema']
                # list_of_data= []
                # for dt in data['data']:
                fill_null = {}
                for field in schema:
                    field_name = field['name']
                    fill_null[field_name] = data['data'].get(field_name, None)
                    # list_of_data.append(fill_null)
                data['data'] = fill_null
            yield data
        except Exception as e:
            print_error(e)
            result = dead_letter_message(
                destination= 'data_processing',
                row = data,
                error_message = e,
                stage='fill_null_data'
            )
            yield TaggedOutput('error',result)    
class read_schema(beam.DoFn):
    """Read schema from avro file stored in GCS.
    
        Return: Arvo schema structure: 

           Result.data -> {
                "name": str,
                "type": str,
                "fields: [],
                "path": str
            }
            Result.error -> dead_letter_message

    """
    def setup(self):
        self.fs = GCSFileSystem(PipelineOptions())
    def process(self, element):
        try:
            with self.fs.open(path=str(element)) as f:
                avro_bytes = f.read()
                f.close()    
            with io.BytesIO(avro_bytes) as avro_file:
                reader = DataFileReader(avro_file, DatumReader()).meta
                parsed = json.loads(reader['avro.schema'].decode())
                parsed['path'] = str(element)
                avro_file.close()
                yield parsed
        except Exception as e:
            print_error(e)
            result = dead_letter_message(
                destination= 'schema_processing',
                row = element,
                error_message = e,
                stage='read_schema'
            )
            yield TaggedOutput('error',result)
class avro_schema_to_bq_schema(beam.DoFn):
    """Process the schema read from GCS, pairs it into Bigquery format.

        Return: Bigquery schema structure: 

           Result.data -> {
                "name": str,
                "type": str,
                "fields: [
                    {
                        "name": str,
                        "type": str,
                        "mode": str,
                    },
                    ...
                ],
                "path": str
            }
            Result.error -> dead_letter_message

    """
    def __init__(self, ignore_fields, convert_columns):
        self.ignore_fields=ignore_fields
        self.complex_converting_columns_whitelist = convert_columns
    # print_debug("schema process for table {}".format(data["name"]))
    def setup(self):
        self.AVRO_TO_BIGQUERY_TYPES = {
        "record": "RECORD",
        "string": "STRING",
        "int": "INTEGER",
        "boolean": "BOOLEAN",
        "double": "FLOAT",
        "float": "FLOAT",
        "long": "INT64",
        "bytes": "BYTES",
        "enum": "STRING",
        "decimal": "FLOAT",
        "uuid": "STRING",
        "date": "TIMESTAMP",
        "time-millis": "TIMESTAMP",
        "time-micros": "TIMESTAMP",
        "timestamp-millis": "TIMESTAMP",
        "timestamp-micros": "TIMESTAMP",
        "varchar": "STRING",
        "number": "STRING",
        "serial": "INT64",
        "bigserial": "INT64",
        "int2": "INT64",
        "int4": "INT64",
        "int8": "INT64",
        "numeric": "NUMERIC",
        "numeric_without_prec_scale": "STRING",
        "float4": "FLOAT64",
        "float8": "FLOAT64",
        "money": "FLOAT64",
        "bytea": "BYTES",
        "varchar": "STRING",
        "bpchar": "STRING",
        "text": "STRING",
        "cidr": "STRING",
        "inet": "STRING",
        "macaddr": "STRING",
        "macaddr8": "STRING",
        "bit": "STRING",
        "uuid": "STRING",
        "xml": "STRING",
        "json": "JSON",
        "jsonb": "JSON",
        "tsvector": "STRING",
        "tsquery": "STRING",
        "timestamp": "TIMESTAMP",
        "timestamptz": "TIMESTAMP",
        "date": "DATE",
        "time": "TIME",
        "timetz": "TIME",
        "interval": "STRING",
        "point": "STRING",
        "line": "STRING",
        "lseg": "STRING",
        "box": "STRING",
        "path": "STRING",
        "polygon": "STRING",
        "circle": "STRING",
        "geometry": "JSON",
        "array": "STRING",
        "composite": "STRING",
        "range": "STRING",
        "oid": "INT64",
        "pg_lsn": "STRING",
        "bool": "BOOL",
        "char": "STRING",
        "name": "STRING",
        "sl_timestamp": "TIMESTAMP"
        }
    def _should_ignore(self,field, parent):
        full_name = f"{parent}.{field}" if parent else field
        return full_name in self.ignore_fields
    def _process_fields(self,fields, parent=""):
        result = []
        for field in fields:
            name = field["name"]
            if self._should_ignore(name, parent):
                continue
            if "type" in field and isinstance(field["type"], dict) and "fields" in field["type"]:
                field["type"]["fields"] = self._process_fields(field["type"]["fields"], f"{parent}.{name}" if parent else name)
            result.append(field)
        return result
    def flatten_schema(self,data):
        new_fields = []
        new_fields.append({"name": "ingestion_meta_data_processing_timestamp", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]})
        for field in data["fields"]:
            field_name = field["name"]

            if field_name == "payload":
                for subfield in field["type"]["fields"]:
                    new_fields.append(subfield)
            elif field_name == "source_metadata":
                for subfield in field["type"]["fields"]:
                    subfield_name = f"source_metadata_{subfield['name']}"
                    subfield["name"] = subfield_name
                    new_fields.append(subfield)
            else:
                prefixed_name = f"ingestion_meta_data_{field_name}"
                field["name"] = prefixed_name
                new_fields.append(field)
        new_fields.append({"name": "ingestion_meta_data_gcs_path", "type": ["null", {"type": "string"}]})
        new_fields.append({"name": "row_hash", "type": ["null", {"type": "string"}]})
        data["fields"] = new_fields
        return data
    def clean_unused_schema_fields(self, data):
        data["fields"] = self._process_fields(data["fields"])
        return data
    def _convert_type(self,avro_type, field_name):
            """
            Convert an Avro type to a BigQuery type
            :param avro_type: The Avro type
            :return: The BigQuery type
            """
            mode = "NULLABLE"
            fields = ()

            if isinstance(avro_type, list):
                # list types are unions, one of them should be null; get the real type
                if len(avro_type) == 2:
                    if avro_type[0] == "null":
                        avro_type = avro_type[1]
                    elif avro_type[1] == "null":
                        avro_type = avro_type[0]
                    else:
                        raise ReferenceError(
                            "One of the union fields should have type `null`"
                        )
                else:
                    raise ReferenceError(
                        "A Union type can only consist of two types, "
                        "one of them should be `null`"
                    )

            if isinstance(avro_type, dict):
                field_type, fields, mode = self._convert_complex_type(avro_type,field_name)

            else:
                field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type]

            return field_type, mode, fields
    def _convert_complex_type(self,avro_type,field_name):
        """
        Convert a Avro complex type to a BigQuery type
        :param avro_type: The Avro type
        :return: The BigQuery type
        """
        fields = ()
        mode = "NULLABLE"

        if avro_type["type"] == "record":
            if field_name in self.complex_converting_columns_whitelist:
                field_type = "RECORD"
                fields = tuple(map(lambda f: self._convert_field(f), avro_type["fields"]))
            else: field_type = "STRING"
        elif avro_type["type"] == "array":
            mode = "REPEATED"
            if "logicalType" in avro_type["items"]:
                field_type = self.AVRO_TO_BIGQUERY_TYPES[
                    avro_type["items"]["logicalType"]
                ]
            elif isinstance(avro_type["items"], dict):
                # complex array
                if avro_type["items"]["type"] == "enum":
                    field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type["items"]["type"]]
                else:
                    field_type = "RECORD"
                    fields = tuple(
                        map(
                            lambda f: self._convert_field(f),
                            avro_type["items"]["fields"],
                        )
                    )
            else:
                # simple array
                field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type["type"]]
        elif avro_type["type"] == "enum":
            field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type["type"]]
        elif avro_type["type"] == "map":
            field_type = "RECORD"
            mode = "REPEATED"
            # Create artificial fields to represent map in BQ
            key_field = {
                "name": "key",
                "type": "string",
                "doc": "Key for map avro field",
            }
            value_field = {
                "name": "value",
                "type": avro_type["values"],
                "doc": "Value for map avro field",
            }
            fields = tuple(
                map(lambda f: self._convert_field(f), [key_field, value_field])
            )
        elif "logicalType" in avro_type:
            field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type["logicalType"]]
        elif avro_type["type"] in self.AVRO_TO_BIGQUERY_TYPES:
            field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type["type"]]
        else:
            raise ReferenceError(f"Unknown complex type {avro_type['type']}")
        return field_type, fields, mode
    def _convert_field(self,avro_field):
        """
        Convert an Avro field to a BigQuery field
        :param avro_field: The Avro field
        :return: The BigQuery field
        """

        if "logicalType" in avro_field:
            field_type, mode, fields = self._convert_type(avro_field["logicalType"],avro_field.get("name"))
        else:
            field_type, mode, fields = self._convert_type(avro_field["type"],avro_field.get("name"))

        return {
            "name": avro_field.get("name"),
            "type": field_type,
            "mode": mode,
            "fields": fields,
        }
    def key_value_mapping(self,data):
        return (data['name'], data['fields'],data['path'])
    def convert_bq_schema(self,data):
        # Function to convert JSON schema to BigQuery schema
        data["fields"] = list(map(lambda f: self._convert_field(f), data["fields"]))
        return data
    def process(self,data):
        try:
            _data = self.clean_unused_schema_fields(data)
            _data = self.flatten_schema(_data)
            yield self.convert_bq_schema(_data)
            # yield(self.key_value_mapping(_data))
        except Exception as e:
            print_error(e)
            result = dead_letter_message(
                destination= 'schema_processing', 
                row = data,
                error_message = e,
                stage='avro_schema_to_bq_schema'
            )
            yield TaggedOutput('error',result)
class avro_processing(beam.DoFn):
    """
        Process the avro content, including cleanup flatten data.

        Return:

            Result.data -> ("table_name", "data")
            Result.error -> dead_letter_message
    """
    def _recursive_remove(self,data, keys):
        if len(keys) == 1:
            data.pop(keys[0], None)
        else:
            key = keys.pop(0)
            if key in data:
                self._recursive_remove(data[key], keys)
                if not data[key]:  # Remove the key if the sub-dictionary is empty
                    data.pop(key)
    def remove_elements(self,data, removelist):
        for item in removelist:
            keys = item.split('.')
            self._recursive_remove(data, keys)
        return data
    def _convert_data(self,value):
        if isinstance(value, datetime.datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S.%f %Z')
        elif isinstance(value,dict):
            return str(value)
        else: return value
    def flatten_data(self,data):
        flattened_data = {}
        flattened_data['ingestion_meta_data_processing_timestamp'] = self._convert_data((datetime.datetime.now(datetime.timezone.utc))) 
        for key, value in data.items():
            if key == "payload":
                for payload_name,payload_val in value.items():
                    value[payload_name] = self._convert_data(payload_val)
                flattened_data.update(value)
            elif key == "source_metadata":
                for sub_key, sub_value in value.items():
                    flattened_data[f"source_metadata_{sub_key}"] = self._convert_data(sub_value)
            else:
                flattened_data[f"ingestion_meta_data_{key}"] = self._convert_data(value)
        # print_debug("processing data for table {}".format(flattened_data["source_metadata_table"]))
        return flattened_data
        
    def calculate_hash_payload(self, data):
        # Function to calculate the hash of a record
        # You can modify the hashing function and encoding as needed
        record_str = str(data['payload'])  # Convert record to string if necessary
        record_hash = hashlib.sha256(record_str.encode('utf-8')).hexdigest()
        data['payload']['row_hash'] = record_hash
        return data
    
    def mapping_table_name(self, data):
        return (data['ingestion_meta_data_object'],data)
    def merge_with_gcs_link_path(self, data):
        data[1]['gcs_path'] = data[0]
        return data[1]
    def process(self,data, ignore_fields):
        try:
            _data = self.merge_with_gcs_link_path(data)
            _data = self.remove_elements(_data,ignore_fields)
            _data = self.calculate_hash_payload(_data)
            _data = self.flatten_data(_data)
            yield self.mapping_table_name(_data)
            # yield _data
        except Exception as e:
            print_error(e)
            result = dead_letter_message(
                destination= 'read_avro_content', 
                row = data,
                error_message = e,
                stage='avro_processing'
            )
            yield TaggedOutput('error',result)

def dead_letter_message(destination,row,error_message,stage):
    """The letter format to return whenever the error orrcurs."""
    return  ("error" ,{
            "destination": destination,
            "row": row,
            "error_message": error_message,
            "stage": stage,
            "timestamp":(datetime.datetime.now(datetime.timezone.utc))
            })
def get_data_maping(path,default_dataset,dataset):
    if isinstance(dataset,dict):
        path = path.replace('gs://','')
        return next((dataset.get(sub_folder) for sub_folder in path.split('/') if sub_folder in dataset), default_dataset)
    else: return dataset