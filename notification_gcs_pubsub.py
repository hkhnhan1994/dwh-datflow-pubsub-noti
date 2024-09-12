from google.cloud import storage
from config.develop import pubsub_config

def create_bucket_notifications(pubsub_config):
    """Creates a notification configuration for a bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name = pubsub_config.get('bucket_name'))
    notification = bucket.notification(topic_name = pubsub_config.get('topic_name'))
    notification.create()

    print(f"Successfully created notification with ID {notification.notification_id} for bucket {bucket_name}")

def print_pubsub_bucket_notification(bucket_name, notification_id):
    """Gets a notification configuration for a bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The ID of the notification
    # notification_id = "your-notification-id"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    notification = bucket.get_notification(notification_id)

    print(f"Notification ID: {notification.notification_id}")
    print(f"Topic Name: {notification.topic_name}")
    print(f"Event Types: {notification.event_types}")
    print(f"Custom Attributes: {notification.custom_attributes}")
    print(f"Payload Format: {notification.payload_format}")
    print(f"Blob Name Prefix: {notification.blob_name_prefix}")
    print(f"Etag: {notification.etag}")
    print(f"Self Link: {notification.self_link}")