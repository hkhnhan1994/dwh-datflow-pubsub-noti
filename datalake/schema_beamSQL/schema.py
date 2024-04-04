from apache_beam import coders
from typing import NamedTuple, Sequence, Optional

class gcs_notification_message(NamedTuple):
    kind: Optional[str]
    id: Optional[str]
    selfLink: Optional[str]
    name: Optional[str]
    bucket: Optional[str]
    generation: Optional[str] 
    metageneration: int
    contentType: Optional[str] 
    timeCreated: Optional[str] 
    updated: Optional[str]
    storageClass: Optional[str] 
    timeStorageClassUpdated: Optional[str]
    size: Optional[str]
    md5Hash: Optional[str] 
    mediaLink: Optional[str]
    crc32c: Optional[str] 
    etag: Optional[str]

class dwh_entity_role_properties (NamedTuple):
    id= Optional[str]
    public_identifier= Optional[str]
    creation_timestamp= Optional[str]
    last_update_timestamp= Optional[str]
    entity_role_public_identifier= Optional[str]
    property_type= Optional[str]
    property_value= Optional[str]
# CDC metadata schema
class source_metadata (NamedTuple):
    schema= Optional[str]
    table= Optional[str]
    is_deleted= Optional[str]
    change_type= Optional[str]
    tx_id= Optional[str]
    lsn= Optional[str]
    primary_keys= Optional[Sequence[str]]


class cdc_dwh_entity_role_properties(NamedTuple):
    uuid: Optional[str]
    read_timestamp: Optional[str]
    source_timestamp: Optional[str]
    object: Optional[str]
    read_method: Optional[str]
    stream_name: Optional[str]
    schema_key: Optional[str]
    sort_keys: Optional[Sequence[str]]
    payload: Optional[dwh_entity_role_properties]
    source_metadata: Optional[source_metadata]


coders.registry.register_coder(source_metadata, coders.RowCoder)
coders.registry.register_coder(dwh_entity_role_properties, coders.RowCoder)
coders.registry.register_coder(cdc_dwh_entity_role_properties, coders.RowCoder)
coders.registry.register_coder(gcs_notification_message, coders.RowCoder)