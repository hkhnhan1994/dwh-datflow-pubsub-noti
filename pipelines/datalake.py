import logging
import apache_beam as beam
from datalake.schema_beamSQL.schema  import (
    gcs_notification_message,
    )
import json, io, gzip
from google.cloud import storage
# print = logging.info

# class classify_table(beam.ptransform):
#     def __init__(self,table_name):
#         self.table_name=table_name
#     def expand(self, pcoll):
#         def filter(element,table_name):
#             return element['source_metadata']['table'] == table_name
#         data = (
#                 pcoll
#                 | beam.Map(json.loads)
#                 | beam.Filter(filter)
#         )
#         return  (data| beam.Map(print))
#         # return data
'''
treat payload as a string
load data into schema =>
 load and get the metadata
 classify by table name
 add the schema once again 
'''