# import logging
import apache_beam as beam
import json
from avro.datafile import DataFileReader
from avro.io import DatumReader
import io
from apache_beam.pvalue import AsIter, AsDict, AsSingleton
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, ReadFromBigQuery
from apache_beam.transforms.window import FixedWindows

from google.cloud import bigquery
import datetime
import hashlib
import logging
# print = logging.info

class read_path_from_pubsub(beam.PTransform):
    def __init__(self, project, subscription):
        self.project=project
        self.subscription =subscription
    def expand(self, pcoll):
        def filter(element):
            return element['size'] != '0'
        def path_former(element):
            # print("get: gs://"+element['bucket']+"/"+element['name'])
            return "gs://"+element['bucket']+"/"+element['name']
        messages = (
                pcoll 
                | "read gcs noti" >> beam.io.ReadFromPubSub(subscription="projects/{}/subscriptions/{}".format(self.project,self.subscription)).with_output_types(bytes) 
        )
        # checking if pub/sub message contained any files
        get_message_contains_file_url =(
             messages
                | "to json" >> beam.Map(json.loads)
                |"check if file arrived" >> beam.Filter(filter)
                | beam.Map(path_former)
                # |"windowtime pubsub" >> beam.WindowInto(FixedWindows(5))
        )
        return get_message_contains_file_url
# as a side input
class arvo_schema_to_BQ_schema(beam.PTransform):
    def __init__(self,ignore_fields):
            self.fs = GCSFileSystem(PipelineOptions())
            self.ignore_fields =ignore_fields
    def expand(self, pcoll):
        def read_schema(element):
            with self.fs.open(path=str(element)) as f:
                avro_bytes = f.read()
                f.close()    
            with io.BytesIO(avro_bytes) as avro_file:
                reader = DataFileReader(avro_file, DatumReader()).meta
                parsed = json.loads(reader['avro.schema'].decode())
                avro_file.close()
                return parsed
                # return (json.dumps(parsed, indent=4, sort_keys=True))
                # return reader['avro.schema'].decode()
        def clean_unused_schema_fields(data, ignore_fields):
            # print("schema process for table {}".format(data["name"]))
            def should_ignore(field, parent):
                full_name = f"{parent}.{field}" if parent else field
                return full_name in ignore_fields

            def process_fields(fields, parent=""):
                result = []
                for field in fields:
                    name = field["name"]
                    if should_ignore(name, parent):
                        continue
                    if "type" in field and isinstance(field["type"], dict) and "fields" in field["type"]:
                        field["type"]["fields"] = process_fields(field["type"]["fields"], f"{parent}.{name}" if parent else name)
                    result.append(field)
                return result

            data["fields"] = process_fields(data["fields"])
            return data
        def flatten_schema(data):
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
            new_fields.append({"name": "row_hash", "type": ["null", {"type": "string"}]})
            data["fields"] = new_fields
            return data
        # Function to convert JSON schema to BigQuery schema
        def convert_bq_schema(data):
            """
            Convert an Avro schema to a BigQuery schema
            :param data: The Avro schema
            :return: The BigQuery schema
            """
            AVRO_TO_BIGQUERY_TYPES = {
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
            }
            def _convert_type(avro_type):
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
                    field_type, fields, mode = _convert_complex_type(avro_type)

                else:
                    field_type = AVRO_TO_BIGQUERY_TYPES[avro_type]

                return field_type, mode, fields
            def _convert_complex_type(avro_type):
                """
                Convert a Avro complex type to a BigQuery type
                :param avro_type: The Avro type
                :return: The BigQuery type
                """
                fields = ()
                mode = "NULLABLE"

                if avro_type["type"] == "record":
                    field_type = "RECORD"
                    fields = tuple(map(lambda f: _convert_field(f), avro_type["fields"]))
                elif avro_type["type"] == "array":
                    mode =  "NULLABLE" #"REPEATED"
                    if "logicalType" in avro_type["items"]:
                        field_type = AVRO_TO_BIGQUERY_TYPES[
                            avro_type["items"]["logicalType"]
                        ]
                    elif isinstance(avro_type["items"], dict):
                        # complex array
                        if avro_type["items"]["type"] == "enum":
                            field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["items"]["type"]]
                        else:
                            field_type = "RECORD"
                            fields = tuple(
                                map(
                                    lambda f: _convert_field(f),
                                    avro_type["items"]["fields"],
                                )
                            )
                    else:
                        # simple array
                        field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["items"]]
                elif avro_type["type"] == "enum":
                    field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["type"]]
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
                        map(lambda f: _convert_field(f), [key_field, value_field])
                    )
                elif "logicalType" in avro_type:
                    field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["logicalType"]]
                elif avro_type["type"] in AVRO_TO_BIGQUERY_TYPES:
                    field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["type"]]
                else:
                    raise ReferenceError(f"Unknown complex type {avro_type['type']}")
                return field_type, fields, mode
            def _convert_field(avro_field):
                """
                Convert an Avro field to a BigQuery field
                :param avro_field: The Avro field
                :return: The BigQuery field
                """

                if "logicalType" in avro_field:
                    field_type, mode, fields = _convert_type(avro_field["logicalType"])
                else:
                    field_type, mode, fields = _convert_type(avro_field["type"])

                return {
                    "name": avro_field.get("name"),
                    "type": field_type,
                    "mode": mode,
                }
            data["fields"] = list(map(lambda f: _convert_field(f), data["fields"]))
            return data
            
        schema = ( # data must be in Arvo format
            pcoll
            |"read schema from arvo file" >> beam.Map(read_schema)  
            # | beam.WindowInto(GlobalWindows())
            |"remove unused schema fields" >> beam.Map(clean_unused_schema_fields,self.ignore_fields)
            |"flatten schema" >> beam.Map(flatten_schema)
            |"convert arvo schema to BQ schema" >> beam.Map(convert_bq_schema)
        )
        return schema
class read_arvo_content(beam.PTransform):
    def __init__(self,ignore_fields):
            self.ignore_fields =ignore_fields
    def expand(self, pcoll):
        def remove_elements(data, removelist):
            def recursive_remove(data, keys):
                if len(keys) == 1:
                    data.pop(keys[0], None)
                else:
                    key = keys.pop(0)
                    if key in data:
                        recursive_remove(data[key], keys)
                        if not data[key]:  # Remove the key if the sub-dictionary is empty
                            data.pop(key)

            for item in removelist:
                keys = item.split('.')
                recursive_remove(data, keys)
            return data
        def flatten_data(data):
            def convert_data(value):
                if isinstance(value, datetime.datetime):
                    return value
                elif isinstance(value,list):
                    return str(value)
                else: return value
            flattened_data = {}
            flattened_data['ingestion_meta_data_processing_timestamp'] = convert_data((datetime.datetime.now(datetime.timezone.utc))) 
            for key, value in data.items():
                if key == "payload":
                    for payload_name,payload_val in value.items():
                        value[payload_name] = convert_data(payload_val)
                    flattened_data.update(value)
                elif key == "source_metadata":
                    for sub_key, sub_value in value.items():
                        flattened_data[f"source_metadata_{sub_key}"] = convert_data(sub_value)
                else:
                    flattened_data[f"ingestion_meta_data_{key}"] = convert_data(value)
            # print("processing data for table {}".format(flattened_data["source_metadata_table"]))
            return flattened_data
        # Function to calculate the hash of a record
        def calculate_hash_payload(data):
            # You can modify the hashing function and encoding as needed
            record_str = str(data['payload'])  # Convert record to string if necessary
            record_hash = hashlib.sha256(record_str.encode('utf-8')).hexdigest()
            data['payload']['row_hash'] = record_hash
            return data
        data = ( # data must be in Arvo format
            pcoll
            | beam.io.ReadAllFromAvro()
            | "remove unrelated fields" >> beam.Map(remove_elements,self.ignore_fields)
            | "calculate row hash on payload" >>  beam.Map(calculate_hash_payload)
            | "flatten data" >> beam.Map(flatten_data)
            # | beam.Map(lambda x: json.dumps(x).encode())
        )
        return data
class mapping_data_to_schema(beam.PTransform):
    def __init__(self, bq_schema):
        super().__init__()
        self.bq_schema = bq_schema
    def expand(self, pcoll):
        def reorder_and_fill_null_data_based_on_schema(element, schema):
            for sch in schema:
                reordered_data = {}
                for field in sch[1]['fields']:
                    field_name = field['name']
                    if field_name in element:
                        reordered_data[field_name] = element[field_name]
                    else:
                        reordered_data[field_name] = None
                yield reordered_data
        
        return (
            pcoll
            |'reorder data based on schema' >> beam.FlatMap(reorder_and_fill_null_data_based_on_schema, schema= AsIter(self.bq_schema))
            
        )
class schema_changes(beam.PTransform):
    def __init__(self,project ,data_set, bq_schema):
        super().__init__()
        self.project=project
        self.data_set=data_set
        self.bq_schema = bq_schema
    def expand(self, pcoll):
        def merge_schema(arvo_schema,bq_schema):
            curent_fields = {field['name'] for field in arvo_schema.get('fields')}
            for bq in bq_schema:
                # print("{} : {}".format(arvo_schema['name'],bq.keys()))
                if bq.get(arvo_schema['name']):
                    existing_fields = {field['name'] for field in bq.get(arvo_schema['name']).get('fields')}
                    union_set = curent_fields.union(existing_fields)
                    # print(union_set)
                    for field in arvo_schema['fields']:
                        if field['name'] not in union_set:
                            arvo_schema["fields"].append(field)
                return arvo_schema
        
        def map_schema_to_table_name(schema):
            # print("{}:{}.{}".format(self.project,self.data_set,schema['name']))
            return ("{}:{}.{}".format(self.project,self.data_set,schema['name']), {"fields":schema["fields"]})

        return (
            pcoll
            |beam.Map(merge_schema, bq_schema = AsIter(self.bq_schema))
            |beam.Map(map_schema_to_table_name)
        )
class read_schema_from_BQ(beam.PTransform):
    def __init__(self,project ,data_set):
        super().__init__()
        self.project=project
        self.data_set=data_set
    def expand(self, pcoll):
        def read_bq(arvo_schema):
            try:
                    # print("read bq table: {}".format(arvo_schema['name']))
                    client = bigquery.Client()
                    dataset_ref = client.dataset(dataset_id=self.data_set, project=self.project)
                    table_ref = dataset_ref.table(arvo_schema['name'])
                    table = client.get_table(table_ref)
                    f = io.StringIO("")
                    client.schema_to_json(table.schema, f)
                    bq_table = json.loads(f.getvalue())
                    return {"{}:{}.{}".format(self.project,self.data_set,arvo_schema['name']):bq_table}
            except:
                print("table {} not found".format(arvo_schema['name']))
                return {arvo_schema['name']:[]}
        return(
            pcoll| beam.Map(read_bq)
        )
        
class write_to_BQ(beam.PTransform):
    def __init__(self,project ,data_set, bq_schema):
        super().__init__()
        self.project=project
        self.data_set=data_set
        self.bq_schema = bq_schema
    def expand(self, pcoll):
        
        to_BQ =(
            pcoll
            |WriteToBigQuery(
                table=lambda x: "{}:{}.{}".format(self.project,self.data_set,x['ingestion_meta_data_object']),
                schema= lambda table ,bq_schema:bq_schema[table],
                schema_side_inputs = (AsDict(self.bq_schema),),
                write_disposition='WRITE_APPEND',
                create_disposition='CREATE_IF_NEEDED',
                # insert_retry_strategy='RETRY_NEVER',
                temp_file_format='AVRO',
                method='STREAMING_INSERTS',
                # with_auto_sharding=True,
                # kms_key,
            )
        )
        
        error_schema = {'fields': [
                {'name': 'destination', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'row', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'}]}
        _ = (to_BQ.failed_rows_with_errors
        | 'Get Errors' >> beam.Map(lambda e: {
                "destination": e[0],
                "row": json.dumps(e[1]),
                "error_message": e[2][0]['message']
                })
        | 'Write Errors' >> WriteToBigQuery(
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
                table="{}:{}.error_log_table".format(self.project,self.data_set),
                schema=error_schema,
        )
        )
        return to_BQ