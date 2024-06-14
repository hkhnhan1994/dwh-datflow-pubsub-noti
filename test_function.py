from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
path ='/hkhnhan-repo/dwh-datflow-pubsub-noti/datastream-postgredwh_entity_role_properties.avro'
with open(path) as avro_file:
    reader = DataFileReader(avro_file, DatumReader()).meta
    parsed = json.loads(reader['avro.schema'])   
    avro_file.close()
    # return (json.dumps(parsed, indent=4, sort_keys=True))
    print(parsed)
    