import fastavro

def count_avro_records(file_path):
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        count = sum(1 for _ in reader)
    return count

file_path = '/home/hkhnhan/Code/dwh-datflow-pubsub-noti/7bb32d069d36f7728b1ca66f812bb4ff24413219_postgresql-backfill_-96622484_7_17761.avro'
record_count = count_avro_records(file_path)
print(f'Number of records: {record_count}')
#   => 275