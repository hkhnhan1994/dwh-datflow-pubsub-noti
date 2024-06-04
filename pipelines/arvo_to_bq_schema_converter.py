# import sys
# sys.path.append("..") # Adds higher directory to python modules path.
from google.cloud import bigquery  # type: ignore

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

    return bigquery.SchemaField(
        avro_field.get("name"),
        field_type,
        mode=mode,
        description=avro_field.get("doc"),
        fields=fields,
    )


def convert_schema(avro_schema: dict):
    """
    Convert an Avro schema to a BigQuery schema
    :param avro_schema: The Avro schema
    :return: The BigQuery schema
    """

    return tuple(map(lambda f: _convert_field(f), avro_schema["fields"]))






#save here
# class arvo_schema_processing(beam.PTransform):
#     def __init__(self):
#             self.fs = GCSFileSystem(PipelineOptions())
#     def expand(self, pcoll):
#         def read_schema(element):
#             with self.fs.open(path=str(element)) as f:
#                 avro_bytes = f.read()
#                 f.close()    
#             with io.BytesIO(avro_bytes) as avro_file:
#                 reader = DataFileReader(avro_file, DatumReader()).GetMeta('avro.schema')
#                 parsed = json.loads(reader)
#                 avro_file.close()
#                 # return (json.dumps(parsed, indent=4, sort_keys=True))
#                 return parsed
#         def flatten_schema(data):
#             new_fields = []
#             new_fields.append({"name": "ingestion_meta_data_processing_timestamp", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]})
#             for field in data["fields"]:
#                 field_name = field["name"]

#                 if field_name == "payload":
#                     for subfield in field["type"]["fields"]:
#                         new_fields.append(subfield)
#                 elif field_name == "source_metadata":
#                     for subfield in field["type"]["fields"]:
#                         subfield_name = f"source_metadata_{subfield['name']}"
#                         subfield["name"] = subfield_name
#                         new_fields.append(subfield)
#                 else:
#                     prefixed_name = f"ingestion_meta_data_{field_name}"
#                     field["name"] = prefixed_name
#                     new_fields.append(field)
#             new_fields.append({"name": "row_hash", "type": ["null", {"type": "bytes"}]})
#             return {"fields": new_fields}
#         # Function to convert JSON schema to BigQuery schema
#         # def json_to_bigquery_schema(json_schema):
#         #     fields = []
#         #     mapping = {
#         #         'boolean': 'BOOL',
#         #         'int': 'INT64',
#         #         'long': 'INT64',
#         #         'float': 'FLOAT64',
#         #         'double': 'FLOAT64',
#         #         'bytes': 'BYTES',
#         #         'string': 'STRING',
#         #         'date': 'TIMESTAMP'
#         #     }
#         #     def get_bq_type(field_type):
#         #         if isinstance(field_type, dict):
#         #             if field_type['type'] == 'array':
#         #                 return 'STRING'  # Arrays are represented as strings in BigQuery
#         #             elif field_type['type'] == 'long' and field_type.get('logicalType') in ['timestamp-millis', 'timestamp-micros']:
#         #                 return 'TIMESTAMP'
#         #             else:
#         #                 return mapping.get(field_type.get('type','string'), 'STRING')
#         #         elif isinstance(field_type, list):
#         #             for t in field_type:
#         #                 if isinstance(t, dict):
#         #                     if t.get('logicalType') in ['timestamp-millis', 'timestamp-micros']:
#         #                         return 'TIMESTAMP'
#         #                     else: return mapping.get(t.get('type','string'), 'STRING') # Default to STRING if type not found
#         #                 else:
                                
#         #         else:
#         #             return mapping.get(field_type, 'STRING')  # Default to STRING if type not found

#         #     for field in json_schema['fields']:
#         #         field_name = field['name']
#         #         field_type = field['type']
                
#         #         # Get the BigQuery type using the helper function
#         #         bq_type = get_bq_type(field_type) # if get_bq_type(field_type) != None else 'STRING' # Default to STRING if type not found
                
#         #         fields.append({'name': field_name, 'type': bq_type, 'mode': 'NULLABLE'})
            
#         #     return {'fields': fields}
#         schema = ( # data must be in Arvo format
#             pcoll
#             | beam.Map(read_schema)
#         )
#         def checking_null_type (json_schema):
#             null_type = []
#             for field in json_schema['fields']:
#                 if (field['type'] is None):
#                     null_type.append(field['name'])
#             print( f"null columns: {null_type}" )
#             return json_schema
#         flattening =(
#             schema
#             |"flatten schema" >> beam.Map(flatten_schema)
#         )
#         bq_schema_convert=(
#             flattening
#             |"convert schema to BQ" >> beam.Map(convert_schema)
#             | beam.Map(checking_null_type)
#             |"dumps to json" >> beam.Map(lambda x: json.dumps(x, sort_keys =True).encode())
#         )
#         return bq_schema_convert