from google.cloud import storage


def create_bucket_notifications(bucket_name,bucket_prefix,topic_project, topic_name):
    """Creates a notification configuration for a bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The name of a topic
    # topic_name = "your-topic-name"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    notification = bucket.notification(topic_name=topic_name,topic_project=topic_project,blob_name_prefix=bucket_prefix)
    notification.create()

    print(f"Successfully created notification with ID {notification.notification_id} for bucket {bucket_name}")

def list_bucket_notifications(bucket_name):
    """Lists notification configurations for a bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    notifications = bucket.list_notifications()

    for notification in notifications:
        print(f"Notification ID: {notification.notification_id}")

def delete_bucket_notification(bucket_name, notification_id):
    """Deletes a notification configuration for a bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The ID of the notification
    # notification_id = "your-notification-id"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    notification = bucket.notification(notification_id=notification_id)
    notification.delete()

    print(f"Successfully deleted notification with ID {notification_id} for bucket {bucket_name}")

# create_bucket_notifications("test_bucket_upvn","datastream-postgres/datastream","pj-bu-dw-data-sbx","gcs_noti")

# gcloud storage buckets notifications create gs://test_bucket_upvn --topic=projects/pj-bu-dw-data-sbx/topics/gcs_noti -p datastream-postgres/datastream
# gcloud storage buckets notifications describe projects/pj-bu-dw-data-sbx/buckets/test_bucket_upvn/notificationConfigs/8
# gcloud storage buckets notifications delete projects/pj-bu-dw-data-sbx/buckets/test_bucket_upvn/notificationConfigs/8