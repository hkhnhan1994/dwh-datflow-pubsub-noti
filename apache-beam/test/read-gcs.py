import json
from google.cloud import pubsub_v1
from typing import Callable
from concurrent import futures

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            futures.result(timeout=60)
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback
data= "hello"
publisher = pubsub_v1.PublisherClient()
publish_future = publisher.publish("projects/pj-bu-dw-data-sbx/topics/gcs_cmd_stream_test", data=data.encode("utf-8"))
publish_future.result()
print("publish topic: {}".format(publish_future.result()))
# publish_future.add_done_callback(get_callback(publish_future, data.encode("utf-8")))
# futures.wait(publish_future, return_when=futures.ALL_COMPLETED)