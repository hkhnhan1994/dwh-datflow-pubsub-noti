"""Transformations that formed the whole pipeline."""

# import logging
import apache_beam as beam
import json
from apache_beam.pvalue import AsDict
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows, GlobalWindows
# from apache_beam.transforms import trigger 
from apache_beam.pvalue import TaggedOutput
from .functions import (
    read_schema, 
    avro_schema_to_bq_schema, 
    read_bq_schema, 
    merge_schema, 
    create_table, 
    avro_processing,
    fill_null_data,
    dead_letter_message
)
import datetime

from config.develop import print_debug,print_error,print_info
class read_path_from_pubsub(beam.PTransform):
    """Ptransform to read data from Pubsub, could add multiple topics and subscriptions.
       The returned values is a file's link path on GCS.
    """
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
            # print_debug("get: gs://"+element['bucket']+"/"+element['name'])
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
class schema_processing(beam.PTransform):
    """Ptransform to process the schema read from GCS. including the schema changes handler."""
    def __init__(self,ignore_fields,project,dataset,error_handler):
            self.ignore_fields =ignore_fields
            self.project = project
            self.dataset = dataset
            self.error_handler=error_handler
    def expand(self, pcoll):
        _schema = ( # data must be in Arvo format
            pcoll
            |"read schema from arvo file" >> beam.ParDo(read_schema()).with_outputs('error', main='schema')
            )
        to_bq_schema =(
            _schema.schema
            |"schema processing" >> beam.ParDo(avro_schema_to_bq_schema(self.ignore_fields)).with_outputs('error', main='schema')
        )
        read_from_bq = (
            to_bq_schema.schema
            |beam.ParDo(read_bq_schema(),self.project,self.dataset).with_outputs('error', main='schema')
        )
        merge_exists_to_current_schema =(
            read_from_bq.schema
            |beam.ParDo(merge_schema()).with_outputs('error', main='schema')
        )
        create_table_if_needed =(
            merge_exists_to_current_schema.schema
            |beam.ParDo(create_table(),self.project, self.dataset).with_outputs('error', main='schema')
        )
        error = (
            (_schema.error,
             to_bq_schema.error, 
             read_from_bq.error, 
             merge_exists_to_current_schema.error,
             create_table_if_needed.error,
             )
            # |"error w1" >> beam.WindowInto(FixedWindows(5))
            # | "write schema process error to channels" >> write_error_to_alert(self.error_handler)
            |beam.Flatten()
        )
        return create_table_if_needed.schema, error
class read_avro_content(beam.PTransform):
    """Ptransform to process the avro's content read from GCS."""
    def __init__(self,ignore_fields,error_handler):
            self.ignore_fields =ignore_fields
            self.error_handler =error_handler
    def expand(self, pcoll):
        # if avro format:
        data =  ( # data must be in Arvo format
            pcoll
            | beam.io.ReadAllFromAvro(with_filename =True)
            | "avro processing" >> beam.ParDo(avro_processing(), self.ignore_fields).with_outputs('error', main='data')
        )
        # _ = (
        #     (data.error,)
        #     | "write data process error to channels" >> write_error_to_alert(self.error_handler)
        # )
        return data
class write_to_BQ(beam.PTransform):
    """Ptransform to write the processed data to Bigquery."""
    def __init__(self,project ,dataset):
        super().__init__()
        self.project=project
        self.dataset=dataset
    def expand(self, pcoll):
        class get_data(beam.DoFn):
            def process(self,data):
                yield data['data']
        class map_schema_to_table_name(beam.DoFn):
            def process(self,data,project,dataset):
                yield ("{}:{}.{}".format(project,dataset,data['data']['ingestion_meta_data_object']), {'fields':data['bq_schema']})
        data = (pcoll
                |beam.ParDo(get_data())
                |"w1" >> beam.WindowInto(FixedWindows(5))
                
                )
        schema =(pcoll
                 |beam.ParDo(map_schema_to_table_name(),self.project, self.dataset)
                 |"w2" >> beam.WindowInto(FixedWindows(5))
                #  | beam.Map(print_debug)
                 )
        to_BQ =(
            data                  
            |WriteToBigQuery(
                table=lambda x: "{}:{}.{}".format(self.project,self.dataset,x['ingestion_meta_data_object']),
                schema= lambda table ,bq_schema:bq_schema[table],
                schema_side_inputs = (AsDict(schema),),
                write_disposition='WRITE_APPEND',
                create_disposition='CREATE_IF_NEEDED',
                insert_retry_strategy='RETRY_NEVER',
                temp_file_format='AVRO',
                method='STREAMING_INSERTS',
                # with_auto_sharding=True,
                # kms_key,
            )
        )
        get_errors = (to_BQ.failed_rows_with_errors
        | 'Get Errors' >> beam.Map(lambda e: ('error',{
                "destination": e[0],
                "row": json.dumps(e[1],indent=4,default=str),
                "error_message": e[2][0]['message'],
                "stage": "write to BQ",
                "timestamp":(datetime.datetime.now(datetime.timezone.utc))
                }))
        )
        return get_errors
class map_new_data_to_bq_schema(beam.PTransform):
    def expand(self, pcoll):
        class flatten(beam.DoFn):
            def process(self,data):
                try:
                    for dt in data[1]['data']:
                        yield ({'data':dt,'bq_schema':data[1]['bq_schema'][0]} )
                except Exception as e:
                    result = dead_letter_message(
                        destination= 'map_new_data_to_bq_schema', 
                        row = data,
                        error_message = e,
                        stage='flatten'
                    )
                    print_debug(e)
                    yield TaggedOutput('error',result)
                    
        get_data = (pcoll|beam.ParDo(flatten()).with_outputs('error', main='data'))
        fill_null = (
            get_data.data
            |beam.ParDo(fill_null_data()).with_outputs('error', main='data')
        )
        error = (
            (get_data.error,fill_null.error)
            # |"error w2" >> beam.WindowInto(FixedWindows(5))
            |beam.Flatten()
        )
        return fill_null.data, error
        
class write_error_to_alert(beam.PTransform):
    """Ptransform to write error to pubsub and Bigquery channels."""
    def __init__(self, config): 
        self.config = config
    def expand(self, pcoll):
        def convert_to_string(data):
            for key, val in data.items():
                data[key]=str(val)
            return data
        flatten_errors = ( pcoll 
                          |"flatten error" >> beam.Flatten()
                          |beam.Map(lambda x: x[-1])
                        #   |beam.FlatMapTuple(lambda x: x)
                        #   |beam.Map(print_debug)
                          )
        pubsub_topic = "projects/{}/topics/{}".format(self.config['chat_channel']['project'],self.config['chat_channel']['topics'])
        to_pubsub =(
            flatten_errors
            | beam.Map(lambda x: x.pop('row'))
            | beam.Map(lambda x: str(x).encode('utf-8'))
            |beam.io.WriteToPubSub(pubsub_topic)
        )
        to_bq_error_log =(
            flatten_errors
            |beam.Map(convert_to_string)
            |beam.io.WriteToBigQuery(
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
                table="{}:{}.{}".format(self.config['bq_channel']['project'],self.config['bq_channel']['dataset'],self.config['bq_channel']['table_id']),
                schema=self.config['bq_channel']['schema'],
        )
        ) 