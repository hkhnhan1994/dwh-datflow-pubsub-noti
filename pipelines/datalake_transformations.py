# import logging
import apache_beam as beam
import json
from avro.datafile import DataFileReader
from avro.io import DatumReader
import io
from apache_beam.pvalue import AsSideInput, AsDict, AsList
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import datetime
import hashlib
# print = logging.info
test_path='datastream-postgres/datastream/cmd_test/public_dwh_entity_role_properties/2024/05/22/08/40/7bb32d069d36f7728b1ca66f812bb4ff24413219_postgresql-backfill_-96622484_0_771.avro'
simulate_pubsub_mess={
  "name": test_path,
  "size": "1234",
  "bucket": "test_bucket_upvn",
}
mess=json.dumps(simulate_pubsub_mess, indent = 4) 

ignore_fields =[
    'stream_name',
    'schema_key',
    'sort_keys',
    'source_metadata.tx_id',
    'source_metadata.lsn',
]
class read_path(beam.PTransform):
    def __init__(self, subscription_path):
        self.subscription_path=subscription_path
    def expand(self, pcoll):
        def filter(element):
            return element['size'] != '0'
        def path_former(element):
            return "gs://"+element['bucket']+"/"+element['name']
        messages = (
                pcoll 
                # | "read gcs noti" >> beam.io.ReadFromPubSub(subscription=self.subscription_path).with_output_types(bytes) 
                | "Create data" >> beam.Create([mess])
        )
        # checking if pub/sub message contained any files
        get_message_contains_file_url =(
             messages
                | "to json" >> beam.Map(json.loads)
                |"check if file arrived" >> beam.Filter(filter)
                | beam.Map(path_former)
        )
        return get_message_contains_file_url
# as a side input
class arvo_schema_processing(beam.PTransform):
    def __init__(self,ignore_fields):
            self.fs = GCSFileSystem(PipelineOptions())
            self.ignore_fields =ignore_fields
    def expand(self, pcoll):
        def read_schema(element):
            with self.fs.open(path=str(element)) as f:
                avro_bytes = f.read()
                f.close()    
            with io.BytesIO(avro_bytes) as avro_file:
                reader = DataFileReader(avro_file, DatumReader()).GetMeta('avro.schema')
                parsed = json.loads(reader)
                avro_file.close()
                # return (json.dumps(parsed, indent=4, sort_keys=True))
                return parsed
        def clean_unused_schema_fields(data, ignore_fields):
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
            new_fields.append({"name": "row_hash", "type": ["null", {"type": "bytes"}]})
            return {"fields": new_fields}
        schema = ( # data must be in Arvo format
            pcoll
            |"read schema from arvo file" >> beam.Map(read_schema)
            |"remove unused schema fields" >> beam.Map(clean_unused_schema_fields,self.ignore_fields)
            |"flatten schema" >> beam.Map(flatten_schema)
            |"dumps to json" >> beam.Map(lambda x: json.dumps(x, sort_keys =True).encode())
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
            def format_datetime(value):
                if isinstance(value, datetime.datetime):
                    return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')
                else: return value
            flattened_data = {}
            flattened_data['ingestion_meta_data_processing_timestamp'] = format_datetime((datetime.datetime.now(datetime.timezone.utc))) 
            for key, value in data.items():
                if key == "payload":
                    for payload_name,payload_val in value.items():
                        value[payload_name] = format_datetime(payload_val)
                    flattened_data.update(value)
                elif key == "source_metadata":
                    for sub_key, sub_value in value.items():
                        flattened_data[f"source_metadata_{sub_key}"] = format_datetime(sub_value)
                else:
                    flattened_data[f"ingestion_meta_data_{key}"] = format_datetime(value)
            return json.dumps(flattened_data,sort_keys =True).encode()
            # return flattened_data
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
        )
        return data
class gcs_arvo_processing(beam.PTransform):
    def __init__(self, subscription_path =[],project ='pj-bu-dw-data-sbx',data_set ='lake_view_cmd'):
        self.subscription_path=subscription_path
        self.project=project
        self.data_set=data_set
    def expand(self, pcoll):
        # cross join is used as an example.
        # def cross_join_with_two_sides(left, rights1, rights2):
        #     for x in rights1:
        #         for y in rights2:
        #             yield (left, x, y)
        def cross_join_with_sides(left, rights):
            for x in rights:
                yield (left, x)
        # Function to convert JSON schema to BigQuery schema
        def convert_bq_schema(avro_schema):
            """
            Convert an Avro schema to a BigQuery schema
            :param avro_schema: The Avro schema
            :return: The BigQuery schema
            """
            AVRO_TO_BIGQUERY_TYPES = {
            "record": "RECORD",
            "string": "STRING",
            "int": "INTEGER",
            "boolean": "BOOL",
            "double": "FLOAT",
            "float": "FLOAT",
            "long": "INT64",
            "bytes": "BYTES",
            "enum": "STRING",
            # logical types
            "decimal": "FLOAT",
            "uuid": "STRING",
            "date": "DATE",
            "time-millis": "TIME",
            "time-micros": "TIME",
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
                    mode = "REPEATED"
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
                        # field_type = 'STRING'
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
                    "fields":fields,
                }
            # bigquery.SchemaField(
            #         avro_field.get("name"),
            #         field_type,
            #         mode=mode,
            #         description=avro_field.get("doc"),
            #         fields=fields,
            #     )
            return {"fields":list(map(lambda f: _convert_field(f), avro_schema["fields"]))}
        def checking_null_type (json_schema):
            null_type = []
            for field in json_schema['fields']:
                if (field['type'] is None):
                    null_type.append(field['name'])
            print( f"null columns: {null_type}" )
            return json_schema
        path =(
            pcoll|"read file path from pubsub" >> read_path(self.subscription_path)
        )
        data = (path|"arvo data processing" >> read_arvo_content(ignore_fields))
         # get schema
        bq_schema = (
            path
            |"arvo schema processing" >> arvo_schema_processing(ignore_fields)
            |beam.Map(lambda x: json.loads(x))
            |"convert arvo schema to BQ schema" >> beam.Map(convert_bq_schema)
            |"error checking" >> beam.Map(checking_null_type)
            )
        result = (
            data
            | 'ApplyCrossJoinWithTwoSides' >> beam.FlatMap(
            cross_join_with_sides,
            rights=beam.pvalue.AsIter(bq_schema),
            # rights2=beam.pvalue.AsIter(table_name)
        )
        )
        return result
class write_to_BQ(beam.PTransform):
    def __init__(self,project ='pj-bu-dw-data-sbx',data_set ='lake_view_cmd'):
        self.project=project
        self.data_set=data_set
    def expand(self, pcoll):
        # to_BQ= (
        #     pcoll
        #     | beam.Map(lambda x: "{}:{}.{}".format(self.project,self.data_set,json.loads(x[0])['source_metadata_table']))
            
        # )
        to_BQ =(
            pcoll | WriteToBigQuery(
        table=lambda x: "{}:{}.{}".format(self.project,self.data_set,json.loads(x[0])['source_metadata_table']),
        schema = lambda x: json.loads(x[1]),
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
        method= WriteToBigQuery.Method.STREAMING_INSERTS,
        # additional_bq_parameters = {
        #   'timePartitioning': {'type': 'DAY'},
        #   'clustering': {'fields': ['country']}}
        # with_auto_sharding=True,
        # kms_key= self.Option['CREATE_DISPOSITION'],
        # method='STREAMING_INSERTS',) 
        )
        )
        return to_BQ