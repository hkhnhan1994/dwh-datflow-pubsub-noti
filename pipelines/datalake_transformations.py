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
from apache_beam.io.gcp.bigquery_tools import  get_bq_tableschema, BigQueryWrapper
from google.cloud import bigquery
import datetime
import hashlib
import logging
# print = logging.info
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
class read_path_from_pubsub(beam.PTransform):
    def __init__(self, project, subscription, file_format = ['avro']):
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
        messages = (
                pcoll 
                | "read gcs noti" >> beam.io.ReadFromPubSub(subscription="projects/{}/subscriptions/{}".format(self.project,self.subscription)).with_output_types(bytes) 
        )
        # checking if pub/sub message contained any files
        get_message_contains_file_url =(
             messages
                | "to json" >> beam.Map(json.loads)
                |"check if file arrived" >> beam.Filter(filter, self.file_format)
                | beam.Map(path_former)
                # |"windowtime pubsub" >> beam.WindowInto(FixedWindows(5))
        )
        return get_message_contains_file_url
# as a side input
class arvo_schema(beam.PTransform):
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
            new_fields.append({"name": "ingestion_meta_data_gcs_path", "type": ["null", {"type": "string"}]})
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
        def key_value_mapping(data):
            return (data['name'], data['fields'])
        schema = ( # data must be in Arvo format
            pcoll
            |"read schema from arvo file" >> beam.Map(read_schema)  
            # | beam.WindowInto(GlobalWindows())
            |"remove unused schema fields" >> beam.Map(clean_unused_schema_fields,self.ignore_fields)
            |"flatten schema" >> beam.Map(flatten_schema)
            |"convert arvo schema to BQ schema" >> beam.Map(convert_bq_schema)
            | "(table name, data) mapping" >> beam.Map(key_value_mapping)
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
        def merge_with_gcs_link_path(data):
            data[1]['gcs_path'] = data[0]
            return data[1]
        # Function to calculate the hash of a record
        def calculate_hash_payload(data):
            # You can modify the hashing function and encoding as needed
            record_str = str(data['payload'])  # Convert record to string if necessary
            record_hash = hashlib.sha256(record_str.encode('utf-8')).hexdigest()
            data['payload']['row_hash'] = record_hash
            return data
        def mapping_table_name(data):
            return (data['ingestion_meta_data_object'],data)

        data = ( # data must be in Arvo format
            pcoll
            | beam.io.ReadAllFromAvro(with_filename =True)
            | "add gsc path into meta data" >> beam.Map(merge_with_gcs_link_path)
            | "remove unrelated fields" >> beam.Map(remove_elements,self.ignore_fields)
            | "calculate row hash on payload" >>  beam.Map(calculate_hash_payload)
            | "flatten data" >> beam.Map(flatten_data)
            | "mapping table name to data" >> beam.Map(mapping_table_name)
            
        )
        return data
class enrich_data(beam.DoFn):
    def process(self, data):
        schema= data[1]['bq_schema'][0]
        list_of_data= []
        for dt in data[1]['data']:
            reordered_data = {}
            for field in schema:
                field_name = field['name']
                if field_name in dt:
                    reordered_data[field_name] = dt[field_name]
                else:
                    reordered_data[field_name] = None
            list_of_data.append(reordered_data)
        data[1]['data'] = list_of_data
        data[1]['bq_schema'] = schema
        # data[1]['bq_schema'][0] = data[1]['bq_schema']['schema']
        yield (data[0],data[1])
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