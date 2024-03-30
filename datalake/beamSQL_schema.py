import typing
from apache_beam import coders
class gcs_notification_message(typing.NamedTuple):
    kind: str
    id: str
    selfLink: str
    name: str
    bucket: str
    generation: str 
    metageneration: int
    contentType: str 
    timeCreated: str 
    updated: str
    storageClass: str 
    timeStorageClassUpdated: str
    size: str
    md5Hash: str 
    mediaLink: str
    crc32c: str 
    etag: str


coders.registry.register_coder(gcs_notification_message, coders.RowCoder)