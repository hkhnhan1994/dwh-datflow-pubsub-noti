# dwh-datflow-pubsub-noti

## create a notificaton gcs pubsub
```Command:
    gcloud storage buckets notifications create gs://test_bucket_upvn --topic=projects/pj-bu-dw-data-sbx/topics/gcs_noti -p datastream-postgres/datastream
```
## check mptofocatopm gcs pubsub
```
gcloud storage buckets notifications describe projects/pj-bu-dw-data-sbx/buckets/test_bucket_upvn/notificationConfigs/id-job-number
```
## delete notification gcs pubsub
```
gcloud storage buckets notifications delete projects/pj-bu-dw-data-sbx/buckets/test_bucket_upvn/notificationConfigs/id-job-number
```

## list down dataflow job
```
gcloud dataflow jobs list
```
## cancel dataflow job
```
gcloud dataflow jobs cancel JOB_ID
```
