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
import logging
print = logging.info
class merge_schema(beam.DoFn):
    """Merge current schema into exists schema."""
    def process(self, merge_schema):
        try:
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
                print(f'extra fields: {diff_list}')
            if bool(diff_list_but_no_update_schema):
                print(f'deleted fields: {diff_list_but_no_update_schema}')
            is_new_table =False if len(_cur_schema) >0 else True
            yield (merge_schema[0],{'schema':merged_schema, 'is_schema_changes':bool(diff_list),'is_new_table':is_new_table })       
        except Exception as e:
                result = dead_letter_message(
                    destination= 'schema_processing', 
                    row = merge_schema,
                    error_message = e,
                    stage='merge_schema'
                )
                yield TaggedOutput('error',result)
class read_bq_schema(beam.DoFn):
    """Read table schema from Bigquery."""
    def setup(self):
        self.client = bigquery.Client()
    def process(self,schema, project, dataset):
        try:
            dataset_ref = self.client.dataset(dataset_id=dataset, project=project)
            table_ref = dataset_ref.table(schema[0])
            table = self.client.get_table(table_ref)
            f = io.StringIO("")
            self.client.schema_to_json(table.schema, f)
            bq_table = json.loads(f.getvalue())
            yield (schema[0],{"bq_schema":bq_table, "avro_schema":schema[1]})
        except:
            try:
                print("not found table {}".format(schema[0]))
                yield (schema[0],{"bq_schema":[], "avro_schema":schema[1]})
        
            except Exception as e:
                result = dead_letter_message(
                    destination= 'schema_processing', 
                    row = schema,
                    error_message = e,
                    stage='read_bq_schema'
                )
                yield TaggedOutput('error',result)
class create_table(beam.DoFn):
    """Create Bigquery table if not exists."""
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
    def process(self,data,project,dataset):
        try:
            if data[1]['is_new_table']: 
                # if new table detected
                schema = get_bq_tableschema({'fields':data[1]['schema']})
                self.bq_table.get_or_create_table(
                    project_id = project,
                    dataset_id = dataset,
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
                        f'{project}.{dataset}.{data[0]}'
                    )
                    table.schema = schema
                    self.bq_table.gcp_bq_client.update_table(table, ['schema'])

                    print('schema changes updated')
            yield (data[0], data[1]['schema'])
        except Exception as e:
            result = dead_letter_message(
                destination= 'schema_processing', 
                row = data,
                error_message = e,
                stage='create_table'
            )
            yield TaggedOutput('error',result)
class enrich_data(beam.DoFn):
    """Filled the missing columns when schema changes happened."""
    def process(self, data):
        try:
            if len(data[1]['bq_schema']) >0:
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
            yield (data[0],data[1])
        except Exception as e:
            result = dead_letter_message(
                destination= 'data_processing', 
                row = data,
                error_message = e,
                stage='enrich_data'
            )
            yield TaggedOutput('error',result)    
class read_schema(beam.DoFn):
    """Read schema from avro file stored in GCS."""
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
                avro_file.close()
                yield parsed
        except Exception as e:
            result = dead_letter_message(
                destination= 'schema_processing',
                row = element,
                error_message = e,
                stage='read_schema'
            )
            yield TaggedOutput('error',result)
class avro_schema_to_bq_schema(beam.DoFn):
    """Process the schema read from GCS, pairs it into Bigquery format."""
    def __init__(self, ignore_fields):
        self.ignore_fields=ignore_fields
    # print("schema process for table {}".format(data["name"]))
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
        # logical types
        "decimal": "FLOAT",
        "uuid": "STRING",
        "date": "TIMESTAMP",
        "time-millis": "TIMESTAMP",
        "time-micros": "TIMESTAMP",
        "timestamp-millis": "TIMESTAMP",
        "timestamp-micros": "TIMESTAMP",
        "varchar": "STRING",
        "number": "STRING"
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
    def _convert_type(self,avro_type):
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
                field_type, fields, mode = self._convert_complex_type(avro_type)

            else:
                field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type]

            return field_type, mode, fields
    def _convert_complex_type(self,avro_type):
        """
        Convert a Avro complex type to a BigQuery type
        :param avro_type: The Avro type
        :return: The BigQuery type
        """
        fields = ()
        mode = "NULLABLE"

        if avro_type["type"] == "record":
            field_type = "RECORD"
            fields = tuple(map(lambda f: self._convert_field(f), avro_type["fields"]))
        elif avro_type["type"] == "array":
            mode =  "NULLABLE" #"REPEATED"
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
                field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type["items"]]
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
        elif avro_type["type"] in self.AVRO_TO_BIGQUERY_TYPES:
            field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type["type"]]
        elif "logicalType" in avro_type:
            field_type = self.AVRO_TO_BIGQUERY_TYPES[avro_type["logicalType"]]
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
            field_type, mode, fields = self._convert_type(avro_field["logicalType"])
        else:
            field_type, mode, fields = self._convert_type(avro_field["type"])

        return {
            "name": avro_field.get("name"),
            "type": field_type,
            "mode": mode,
        }
    def key_value_mapping(self,data):
        return (data['name'], data['fields'])
    def convert_bq_schema(self,data):
        # Function to convert JSON schema to BigQuery schema
        data["fields"] = list(map(lambda f: self._convert_field(f), data["fields"]))
        return data
    def process(self,data):
        try:
            _data = self.clean_unused_schema_fields(data)
            _data = self.flatten_schema(_data)
            _data = self.convert_bq_schema(_data)
            yield(self.key_value_mapping(_data))
        except Exception as e:
            result = dead_letter_message(
                destination= 'schema_processing', 
                row = data,
                error_message = e,
                stage='avro_schema_to_bq_schema'
            )
            yield TaggedOutput('error',result)
class avro_processing(beam.DoFn):
    """Process the avro content, including cleanup flatten data."""
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
            return value
        elif isinstance(value,list):
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
        # print("processing data for table {}".format(flattened_data["source_metadata_table"]))
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
        except Exception as e:
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