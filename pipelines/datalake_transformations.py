# import logging
import apache_beam as beam
import json
from avro.datafile import DataFileReader
from avro.io import DatumReader
import io
from apache_beam.pvalue import AsSingleton
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
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
    def __init__(self):
            self.fs = GCSFileSystem(PipelineOptions())
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
        # Function to convert JSON schema to BigQuery schema
        def json_to_bigquery_schema(json_schema):
            fields = []
            mapping = {
                'boolean': 'BOOL',
                'int': 'INT64',
                'long': 'INT64',
                'float': 'FLOAT64',
                'double': 'FLOAT64',
                'bytes': 'BYTES',
                'string': 'STRING',
                'date': 'TIMESTAMP'
            }
            def get_bq_type(field_type):
                if isinstance(field_type, dict):
                    if field_type['type'] == 'array':
                        return 'STRING'  # Arrays are represented as strings in BigQuery
                    elif field_type['type'] == 'long' and field_type.get('logicalType') in ['timestamp-millis', 'timestamp-micros']:
                        return 'TIMESTAMP'
                    else:
                        return mapping.get(field_type.get('type','string'), 'STRING')
                elif isinstance(field_type, list):
                    for t in field_type:
                        if isinstance(t, dict):
                            if t.get('logicalType') in ['timestamp-millis', 'timestamp-micros']:
                                return 'TIMESTAMP'
                            else: return mapping.get(t.get('type','string'), 'STRING') # Default to STRING if type not found
                else:
                    return mapping.get(field_type, 'STRING')  # Default to STRING if type not found

            for field in json_schema['fields']:
                field_name = field['name']
                field_type = field['type']
                
                # Get the BigQuery type using the helper function
                bq_type = get_bq_type(field_type)
                
                fields.append({'name': field_name, 'type': bq_type, 'mode': 'NULLABLE'})
            
            return {'fields': fields}
        schema = ( # data must be in Arvo format
            pcoll
            | beam.Map(read_schema)
        )
        flattening =(
            schema
            |"flatten schema" >> beam.Map(flatten_schema)
        )
        bq_schema_convert=(
            flattening
            |"convert schema to BQ" >> beam.Map(json_to_bigquery_schema)
        )
        return bq_schema_convert
class read_arvo_content(beam.PTransform):
    def expand(self, pcoll):
        data = ( # data must be in Arvo format
            pcoll
            | beam.io.ReadAllFromAvro()
        )
        return data
class gcs_arvo_processing(beam.PTransform):
    def __init__(self, subscription_path,project ='pj-bu-dw-data-sbx',data_set ='lake_view_cmd'):
        self.subscription_path=subscription_path
        self.project=project
        self.data_set=data_set
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
            flattened_data = {}
            flattened_data['ingestion_meta_data_processing_timestamp'] = datetime.datetime.now(datetime.timezone.utc)
            for key, value in data.items():
                if key == "payload":
                    flattened_data.update(value)
                elif key == "source_metadata":
                    for sub_key, sub_value in value.items():
                        flattened_data[f"source_metadata_{sub_key}"] = sub_value
                else:
                    flattened_data[f"ingestion_meta_data_{key}"] = value
            return flattened_data
        # Function to calculate the hash of a record
        def calculate_hash_payload(data):
            # You can modify the hashing function and encoding as needed
            
            record_str = str(data['payload'])  # Convert record to string if necessary
            record_hash = hashlib.sha256(record_str.encode('utf-8')).hexdigest()
            data['payload']['row_hash'] = record_hash
            return data
        # cross join is used as an example.
        def cross_join_with_two_sides(left, rights1, rights2):
            for x in rights1:
                for y in rights2:
                    yield (left, x, y)
        path =(
            pcoll| read_path(self.subscription_path)
        )
        data = (path| read_arvo_content())
        ignore_unrelated_fields =(
            data
            | "remove unrelated fields" >> beam.Map(remove_elements,ignore_fields)   
        )
        hashing_payload = (
           ignore_unrelated_fields
           |  beam.Map(calculate_hash_payload)
        )
        flattening_data =(
            hashing_payload
            | "flatten data" >> beam.Map(flatten_data)   
        )
        # get table name
        table_name = (
            flattening_data
            | "get table name" >> beam.Map(lambda x:x['source_metadata_table'])
        )
         # get schema
        bq_schema = (
            path
            | arvo_schema_processing()
            )
        result = (
            flattening_data
            | 'ApplyCrossJoinWithTwoSides' >> beam.FlatMap(
            cross_join_with_two_sides,
            rights1=beam.pvalue.AsIter(bq_schema),
            rights2=beam.pvalue.AsIter(table_name)
        )
        )
        return result
class write_to_BQ(beam.PTransform):
    def __init__(self,project ='pj-bu-dw-data-sbx',data_set ='lake_view_cmd'):
        self.project=project
        self.data_set=data_set
    def expand(self, pcoll):
        def extract_schema(element):
            return element[1]
        def extract_table_name(element):
            return "{}.{}.{}".format(self.project,self.data_set,element[2])
        data = (pcoll |beam.Map(lambda x:x[0]))
        schema = (pcoll |beam.Map(lambda x:x[1]))
        table_name = (pcoll |beam.Map(lambda x: "{}.{}.{}".format(self.project,self.data_set,x[2])))
        write_to_BQ =(
            data
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            # table = beam.pvalue.AsSingleton(table_name),
            table=lambda _: table_name,
            schema=lambda _: schema,
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
            ignore_unknown_columns=True,
            additional_bq_parameters='ADDITIONAL_BQ_PARAMETERS',
            with_auto_sharding=True,
                # kms_key= self.Option['CREATE_DISPOSITION'],
                # method='STREAMING_INSERTS',) 
        )
        )
        return write_to_BQ