# import logging
import apache_beam as beam
import json
from avro.datafile import DataFileReader
from avro.io import DatumReader
import io
from apache_beam.pvalue import AsDict
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows
from apache_beam.pvalue import TaggedOutput
# from apache_beam.io.gcp.bigquery_tools import  get_bq_tableschema, BigQueryWrapper
# from google.cloud import bigquery
from .functions import dead_letter_message
import datetime
import hashlib
import logging
# print = logging.info
class read_path_from_pubsub(beam.PTransform):
    def __init__(self, project: str, subscription: list, file_format = ['avro']):
        self.project=project
        self.subscription =subscription
        self.file_format = file_format
    def expand(self, pcoll):
        def filter(element, file_format):
            check_format = element['name'].split('/')[-1]
            for f in file_format:
                if check_format.endswith(f):
                    return element
        def path_former(element):
            # print("get: gs://"+element['bucket']+"/"+element['name'])
            return "gs://"+element['bucket']+"/"+element['name']

        subs = [beam.io.PubSubSourceDescriptor("projects/{}/subscriptions/{}".format(self.project,sub)) for sub in self.subscription]
        get_message_contains_file_url =(
                pcoll
                | "read gcs noti" >> beam.io.MultipleReadFromPubSub(subs).with_output_types(bytes) 
                | "to json" >> beam.Map(json.loads)
                |"check if file arrived" >> beam.Filter(filter, self.file_format)
                | beam.Map(path_former)
                # |"windowtime pubsub" >> beam.WindowInto(FixedWindows(5))
        )
        return get_message_contains_file_url
class avro_schema(beam.PTransform):
    def __init__(self,ignore_fields):
            self.ignore_fields =ignore_fields
    def expand(self, pcoll):
        class read_schema(beam.DoFn):
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
                        destination= 'read_schema',
                        row = element,
                        error_message = e,
                        stage='avro_schema'
                    )
                    yield TaggedOutput('error',result)
        class schema_processing(beam.DoFn):
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
                except Exception as e:
                    result = dead_letter_message(
                        destination= 'schema_processing',
                        row = data,
                        error_message = e,
                        stage='clean_unused_schema_fields'
                    )
                    yield TaggedOutput('error',result)
                try:
                    _data = self.flatten_schema(_data)
                except Exception as e:
                    result = dead_letter_message(
                        destination= 'schema_processing',
                        row = _data,
                        error_message = e,
                        stage='flatten_schema'
                    )
                    yield TaggedOutput('error',result)
                try:
                    _data = self.convert_bq_schema(_data)
                except Exception as e:
                    result = dead_letter_message(
                        destination= 'schema_processing',
                        row = _data,
                        error_message = e,
                        stage='convert_bq_schema'
                    )
                    yield TaggedOutput('error',result)
                try:
                    yield(self.key_value_mapping(_data))
                except Exception as e:
                    result = dead_letter_message(
                        destination= 'schema_processing',
                        row = _data,
                        error_message = e,
                        stage='key_value_mapping'
                    )
                    yield TaggedOutput('error',result)
        _schema = ( # data must be in Arvo format
            pcoll
            |"read schema from arvo file" >> beam.ParDo(read_schema()).with_outputs('error', main='schema')
            )
        
        processing =(
            _schema.schema
            |"schema processing" >> beam.ParDo(schema_processing(self.ignore_fields)).with_outputs('error', main='schema')
        )
        # | beam.WindowInto(GlobalWindows())
        return processing.schema
class read_avro_content(beam.PTransform):
    def __init__(self,ignore_fields):
            self.ignore_fields =ignore_fields
    def expand(self, pcoll):
        class avro_processing(beam.DoFn):
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
                _data = self.merge_with_gcs_link_path(data)
                _data = self.remove_elements(_data,ignore_fields)
                _data = self.calculate_hash_payload(_data)
                _data = self.flatten_data(_data)
                yield self.mapping_table_name(_data)
        # if avro format:
        return ( # data must be in Arvo format
            pcoll
            | beam.io.ReadAllFromAvro(with_filename =True)
            | "avro processing" >> beam.ParDo(avro_processing(), self.ignore_fields)
        )
class write_to_BQ(beam.PTransform):
    def __init__(self,project ,dataset):
        super().__init__()
        self.project=project
        self.dataset=dataset
    def expand(self, pcoll):
        class get_data(beam.DoFn):
            def process(self,data):
                for dt in data[1]['data']:
                    yield dt
        class map_schema_to_table_name(beam.DoFn):
            def process(self,data,project,dataset):
                yield ("{}:{}.{}".format(project,dataset,data[0]), data[1]['bq_schema'])
        data = (pcoll
                |beam.ParDo(get_data())
                |"w1" >> beam.WindowInto(FixedWindows(5))
                
                )
        schema =(pcoll
                 |beam.ParDo(map_schema_to_table_name(),self.project, self.dataset)
                 |"w2" >> beam.WindowInto(FixedWindows(5))
                #  | beam.Map(print)
                 )
        to_BQ =(
            data                  
            |WriteToBigQuery(
                table=lambda x: "{}:{}.{}".format(self.project,self.dataset,x['ingestion_meta_data_object']),
                schema= lambda table ,bq_schema:bq_schema[table],
                schema_side_inputs = (AsDict(schema),),
                write_disposition='WRITE_APPEND',
                create_disposition='CREATE_NEVER',
                insert_retry_strategy='RETRY_NEVER',
                temp_file_format='AVRO',
                method='STREAMING_INSERTS',
                # with_auto_sharding=True,
                # kms_key,
            )
        )
        error_schema = {'fields': [
                {'name': 'destination', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'row', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
                ]}
        _ = (to_BQ.failed_rows_with_errors
        | 'Get Errors' >> beam.Map(lambda e: {
                "destination": e[0],
                "row": json.dumps(e[1],indent=4,default=str),
                "error_message": e[2][0]['message'],
                "timestamp":(datetime.datetime.now(datetime.timezone.utc))
                })
        | 'Write Errors' >> WriteToBigQuery(
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
                table="{}:{}.error_log_table".format(self.project,self.dataset),
                schema=error_schema,
        )
        )
        return to_BQ