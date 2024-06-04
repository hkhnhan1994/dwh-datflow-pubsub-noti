from google.api_core import retry
from google.cloud import pubsub_v1
import json
def pull_subs(project_id,subscription_id,mess_number):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)   
    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        # The subscriber pulls a specific number of messages. The actual
        # number of messages pulled may be smaller than max_messages.
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": mess_number},
            retry=retry.Retry(deadline=300),
        )

        if len(response.received_messages) == 0:
            return

        ack_ids = []
        for received_message in response.received_messages:
            print(f"Received: {received_message.message.data}.")
            ack_ids.append(received_message.ack_id)

        # Acknowledges the received messages so they will not be sent again.
        # subscriber.acknowledge(
        #     request={"subscription": subscription_path, "ack_ids": ack_ids}
        # )

        # print(
        #     f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
        # )
        return response
project_id = "pj-bu-dw-data-sbx"
subscription_id = "gcs_cmd_stream_test"
NUM_MESSAGES = 20
response=pull_subs(project_id,subscription_id,NUM_MESSAGES)
for mes in response.received_messages:
    decoded_string = (mes.message.data).decode("utf-8")
    decoded_string=json.loads(decoded_string)
    print("--------------------------------")
    for d in decoded_string.items():
        print(d)
