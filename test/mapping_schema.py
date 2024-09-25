data = {
  "type": "record",
  "name": "public_dwh_natural_person_payment_accounts",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "multidimensionalArray_tags",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "multidimensionalArray_tags"
                }
              ],
              "name": "nestedArray",
              "default": "null"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "elementValue",
              "default": "null"
            }
          ]
        }
      ],
      "name": "tags"
    }
  ]
}
class convert():
    def __init__(self):
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
            field_type, mode, fields = self._convert_type(avro_field["logicalType"])
        else:
            field_type, mode, fields = self._convert_type(avro_field["type"])

        return {
            "name": avro_field.get("name"),
            "type": field_type,
            "mode": mode,
            "fields": fields
        }
    def key_value_mapping(self,data):
        return (data['name'], data['fields'])
    def convert_bq_schema(self,data):
        # Function to convert JSON schema to BigQuery schema
        data["fields"] = list(map(lambda f: self._convert_field(f), data["fields"]))
        return data
    
converter = convert()

print(converter.convert_bq_schema(data))