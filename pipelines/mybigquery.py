
import random
import uuid
from objsize import get_deep_size
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.gcp import bigquery_tools
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms.util import ReshufflePerKey
from apache_beam.transforms.window import GlobalWindows
from config.develop import print_debug,print_error,print_info
from .functions import dead_letter_message
from apache_beam.io.gcp.bigquery import (
    BigQueryDisposition,
    BigQueryWriteFn, 
    WriteResult,
    MAX_INSERT_PAYLOAD_SIZE, 
    MAX_INSERT_RETRIES,
    DEFAULT_BATCH_BUFFERING_DURATION_LIMIT_SEC
    )
from apache_beam.pvalue import TaggedOutput
class my_BigQueryWriteFn(BigQueryWriteFn):
    def process(self, element):
        content = element[1]
        schema = content[-1][0]['bq_schema']['schema']
        data = [(ele[0]['data'],ele[1]) for ele in content]
        destination = bigquery_tools.get_hashable_destination(element[0])
        # schema = self.schema
        self._create_table_if_needed(
            bigquery_tools.parse_table_reference(destination), schema)
        if not self.with_batched_input:
            print_debug("not auto sharding")
            # row_and_insert_id = element[1]
            row_and_insert_id = (id,data)
            row_byte_size = get_deep_size(row_and_insert_id)

            # send large rows that exceed BigQuery insert limits to DLQ
            if row_byte_size >= self._max_insert_payload_size:
                row_mb_size = row_byte_size / 1_000_000
                max_mb_size = self._max_insert_payload_size / 1_000_000
                error = (
                    f"Received row with size {row_mb_size}MB that exceeds "
                    f"the maximum insert payload size set ({max_mb_size}MB).")
                return [
                    pvalue.TaggedOutput(
                        my_BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS,
                        GlobalWindows.windowed_value(
                            (destination, row_and_insert_id[0], error))),
                    pvalue.TaggedOutput(
                        my_BigQueryWriteFn.FAILED_ROWS,
                        GlobalWindows.windowed_value(
                            (destination, row_and_insert_id[0])))
                ]

            # Flush current batch first if adding this row will exceed our limits
            # limits: byte size; number of rows
            if ((self._destination_buffer_byte_size[destination] + row_byte_size >
                self._max_insert_payload_size) or
                len(self._rows_buffer[destination]) >= self._max_batch_size):
                flushed_batch = self._flush_batch(destination)
                # After flushing our existing batch, we now buffer the current row
                # for the next flush
                self._rows_buffer[destination].append(row_and_insert_id)
                self._destination_buffer_byte_size[destination] = row_byte_size
                return flushed_batch

            self._rows_buffer[destination].append(row_and_insert_id)
            self._destination_buffer_byte_size[destination] += row_byte_size
            self._total_buffered_rows += 1
            if self._total_buffered_rows >= self._max_buffered_rows:
                return self._flush_all_batches()
        else:
            # The input is already batched per destination, flush the rows now.
            # batched_rows = data
            self._rows_buffer[destination].extend(data)
            try:
                return self._flush_batch(destination)
            except Exception as e:
                print_error(e)
                result = dead_letter_message(
                destination= 'WriteToBigQuery', 
                row = element,
                error_message = e,
                stage='my_BigQueryWriteFn')
                return TaggedOutput('error',result)
class _StreamToBigQuery(PTransform):
  def __init__(
      self,
      schema,
      batch_size,
      create_disposition,
      write_disposition,
      kms_key,
      retry_strategy,
      additional_bq_parameters,
      triggering_frequency,
      ignore_insert_ids,
      ignore_unknown_columns,
      with_auto_sharding,
      test_client=None,
      max_retries=None,
      max_insert_payload_size=MAX_INSERT_PAYLOAD_SIZE):
    self.schema = schema
    self.batch_size = batch_size
    self.create_disposition = create_disposition
    self.write_disposition = write_disposition
    self.kms_key = kms_key
    self.retry_strategy = retry_strategy
    self.test_client = test_client
    self.additional_bq_parameters = additional_bq_parameters
    self.ignore_insert_ids = ignore_insert_ids
    self.ignore_unknown_columns = ignore_unknown_columns
    self.with_auto_sharding = with_auto_sharding
    self.max_retries = max_retries or MAX_INSERT_RETRIES
    self._max_insert_payload_size = max_insert_payload_size
    self.triggering_frequency = triggering_frequency
  class InsertIdPrefixFn(DoFn):
    def start_bundle(self):
      self.prefix = str(uuid.uuid4())
      self._row_count = 0

    def process(self, element):
      key = element[0]
      value = element[1]
      insert_id = '%s-%s' % (self.prefix, self._row_count)
      self._row_count += 1
      yield (key, (value, insert_id))

  def expand(self, input):
    bigquery_write_fn = my_BigQueryWriteFn(
        schema=self.schema,
        batch_size=self.batch_size,
        create_disposition=self.create_disposition,
        write_disposition=self.write_disposition,
        kms_key=self.kms_key,
        retry_strategy=self.retry_strategy,
        test_client=self.test_client,
        additional_bq_parameters=self.additional_bq_parameters,
        ignore_insert_ids=self.ignore_insert_ids,
        ignore_unknown_columns=self.ignore_unknown_columns,
        with_batched_input=self.with_auto_sharding,
        max_retries=self.max_retries,
        max_insert_payload_size=self._max_insert_payload_size)

    def _add_random_shard(element):
      key = element[0]
      value = element[1]
      return ((key, random.randint(0, self._num_streaming_keys)), value)

    def _restore_table_ref(sharded_table_ref_elems_kv):
      sharded_table_ref = sharded_table_ref_elems_kv[0]
      table_ref = bigquery_tools.parse_table_reference(sharded_table_ref)
      return (table_ref, sharded_table_ref_elems_kv[1])
    class AppendDestinationsFn(DoFn):
        def process(self, element):
            # dest = bigquery_tools.parse_table_reference(element['bq_schema']['datalake_maping']['project'],
            #     element['bq_schema']['datalake_maping']['dataset'],
            #     element['bq_schema']['datalake_maping']['table'])
            table_path = "{}:{}.{}".format(element['bq_schema']['datalake_maping']['project'],
                element['bq_schema']['datalake_maping']['dataset'],
                element['bq_schema']['datalake_maping']['table']
                ) 
            yield (table_path, element)
    tagged_data = (
        input
        | 'AppendDestination' >> beam.ParDo(AppendDestinationsFn())
        | 'AddInsertIds' >> beam.ParDo(_StreamToBigQuery.InsertIdPrefixFn())
        | 'ToHashableTableRef' >> beam.Map(bigquery_tools.to_hashable_table_ref))

    if not self.with_auto_sharding:
      tagged_data = (
          tagged_data
          | 'WithFixedSharding' >> beam.Map(_add_random_shard)
          | 'CommitInsertIds' >> ReshufflePerKey()
          | 'DropShard' >> beam.Map(lambda kv: (kv[0][0], kv[1])))
    else:
      # Auto-sharding is achieved via GroupIntoBatches.WithShardedKey
      # transform which shards, groups and at the same time batches the table
      # rows to be inserted to BigQuery.

      # Firstly the keys of tagged_data (table references) are converted to a
      # hashable format. This is needed to work with the keyed states used by
      # GroupIntoBatches. After grouping and batching is done, original table
      # references are restored.
      tagged_data = (
          tagged_data
          | 'WithAutoSharding' >> beam.GroupIntoBatches.WithShardedKey(
              (self.batch_size or my_BigQueryWriteFn.DEFAULT_MAX_BUFFERED_ROWS),
              self.triggering_frequency or
              DEFAULT_BATCH_BUFFERING_DURATION_LIMIT_SEC)
          | 'DropShard' >> beam.Map(lambda kv: (kv[0].key, kv[1])))

    return (
        tagged_data
        | 'FromHashableTableRef' >> beam.Map(_restore_table_ref)
        | 'StreamInsertRows' >> ParDo(
            bigquery_write_fn).with_outputs(
                'error',
                main='main'))
    
class WriteToBigQuery(PTransform):
  """Write data to BigQuery.

  This transform receives a PCollection of elements to be inserted into BigQuery
  tables. The elements would come in as Python dictionaries, or as `TableRow`
  instances.
  """
  def __init__(
      self,
      schema =None,
      create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=BigQueryDisposition.WRITE_APPEND,
      kms_key=None,
      batch_size=None,
      test_client=None,
      insert_retry_strategy=None,
      additional_bq_parameters=None,
      ignore_insert_ids=False,
      # TODO(https://github.com/apache/beam/issues/20712): Switch the default
      # when the feature is mature.
      with_auto_sharding=False,
      ignore_unknown_columns=False,
      max_insert_payload_size=MAX_INSERT_PAYLOAD_SIZE,
      triggering_frequency = None
      ):
    self.create_disposition = BigQueryDisposition.validate_create(
        create_disposition)
    self.write_disposition = BigQueryDisposition.validate_write(
        write_disposition)
    self.batch_size = batch_size
    self.kms_key = kms_key
    self.test_client = test_client
    self.schema = schema
    self.with_auto_sharding = with_auto_sharding
    self.insert_retry_strategy = insert_retry_strategy
    self.additional_bq_parameters = additional_bq_parameters or {}
    self._ignore_insert_ids = ignore_insert_ids
    self._ignore_unknown_columns = ignore_unknown_columns
    self._max_insert_payload_size = max_insert_payload_size
    self.triggering_frequency = triggering_frequency
  # Dict/schema methods were moved to bigquery_tools, but keep references
  # here for backward compatibility.
  get_table_schema_from_string = \
      staticmethod(bigquery_tools.get_table_schema_from_string)
  table_schema_to_dict = staticmethod(bigquery_tools.table_schema_to_dict)
  get_dict_table_schema = staticmethod(bigquery_tools.get_dict_table_schema)

  def expand(self, pcoll):
    outputs = pcoll | _StreamToBigQuery(
        schema=self.schema,
        batch_size=self.batch_size,
        create_disposition=self.create_disposition,
        write_disposition=self.write_disposition,
        kms_key=self.kms_key,
        retry_strategy=self.insert_retry_strategy,
        additional_bq_parameters=self.additional_bq_parameters,
        ignore_insert_ids=self._ignore_insert_ids,
        ignore_unknown_columns=self._ignore_unknown_columns,
        with_auto_sharding=self.with_auto_sharding,
        test_client=self.test_client,
        max_insert_payload_size=self._max_insert_payload_size,
        triggering_frequency = self.triggering_frequency
        )

    return WriteResult(
        method='STREAMING_INSERTS',
        failed_rows=outputs[my_BigQueryWriteFn.FAILED_ROWS],
        failed_rows_with_errors=outputs[
            my_BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS])