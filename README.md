# Data Lake Processing Pipeline

This document provides an overview of the data processing pipeline from Datastream to Google Cloud Storage (GCS), through Google Cloud Dataflow, and into BigQuery.

## Pipeline Overview

1. **Datastream to GCS**: Data is streamed from Datastream and written to Google Cloud Storage.
2. **GCS Notification**: A notification is triggered by changes in the GCS bucket.
3. **Dataflow**: Dataflow processes the data based on the notification and transforms it.
4. **BigQuery**: Processed data is loaded into BigQuery for Data Warehouse.

## Setup Instructions

### Install Dependencies

Install the required Python packages by executing the following commands:

```bash
pip install -r requirements.txt 
pip install google-auth-httplib2
pip install --upgrade 'apache-beam[gcp]'
```

### Configure GCS Bucket Notifications

Create a notification for your GCS bucket to trigger the pipeline when new data arrives. Replace `[bucket name]`, `[project name]`, and `[topic name]` with your actual bucket name, project name, and topic name respectively.

```bash
gcloud storage buckets notifications create gs://[bucket name] --topic=projects/[project name]/topics/[topic name]
```

**Example:**

```bash
gcloud storage buckets notifications create gs://upg-data-sbx-eu-datastream-documents --topic=projects/pj-bu-dw-data-sbx/topics/gcs_noti -p datastream-postgres/datastream/cmd_test
```

### Configure Pipeline Settings

1. Open the configuration file located in the `config/` directory.
2. Edit the environment-specific configuration file, e.g., `config/develop.py`, to set up the pipeline.

### Running the Pipeline

To run the pipeline locally or on Dataflow:

1. Update the `runner` setting in the configuration file to specify your desired execution environment (e.g., local or Dataflow).
- config/[environment].py -> beam_config-> runner
- Local runner: DirectRunner
- Dataflow runner: DataflowRunner
2. Config setup.py if using dataflow runner
2. Execute the pipeline script:

    ```bash
    python3 main.py
    ```

### PostgreSQL Setup for Testing

#### Access PostgreSQL

```bash
sudo -i -u postgres
```

#### Create User

```sql
psql
CREATE USER db_cmd_user PASSWORD '123456';
```

#### Create Schema for User

```sql
CREATE SCHEMA IF NOT EXISTS cmd_schema AUTHORIZATION db_cmd_user;
```

#### Assign Datastream Settings

```sql
ALTER USER db_cmd_user WITH SUPERUSER;
CREATE DATABASE test1 OWNER db_cmd_user;
GRANT ALL PRIVILEGES ON DATABASE test1 TO db_cmd_user;
```

#### Access Database as User

```bash
psql -U db_cmd_user -d test1 -h localhost -p 5432
```

#### Create a Database

```bash
psql
ALTER USER db_cmd_user SUPERUSER;
CREATE DATABASE cmd_db;
```

#### Access Datastream User

```bash
psql -U datastream_user -d test1 -h localhost -p 5432
```

#### Replication Setup

Follow the [official documentation](https://cloud.google.com/datastream/docs/configure-your-source-postgresql-database) to configure replication:

```sql
CREATE PUBLICATION cmd_replicaton_test FOR ALL TABLES;
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('cmd_replicaton_test', 'pgoutput');
```

#### Create Stream User

```sql
CREATE USER db_cmd_user_stream WITH ENCRYPTED PASSWORD '123456';
```

#### Grant Replication Privileges

```sql
GRANT REPLICATION TO db_cmd_user_stream;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_cmd_user_stream;
GRANT USAGE ON SCHEMA public TO db_cmd_user_stream;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO db_cmd_user_stream;
```

#### Import Data from CSV

```sql
COPY "dwh_payment_accounts"(id, public_identifier, creation_timestamp, last_update_timestamp, activated_timestamp, ended_timestamp, info_valid_from_timestamp, identifier, scheme, country, currency, bank_scheme, bank_identifier, status, type)
FROM '/home/hkhnhan/Code/dataflow/data_test/db_extraction/dwh_payment_accounts' DELIMITER ',' CSV;
```

#### Check User Ownership

```sql
SELECT d.datname AS "Name",
pg_catalog.pg_get_userbyid(d.datdba) AS "Owner"
FROM pg_catalog.pg_database d
WHERE d.datname = 'cmd_db'
ORDER BY 1;
```

#### Clean Up

```sql
REASSIGN OWNED BY db_cmd_user_stream TO postgres;
DROP OWNED BY db_cmd_user_stream;
DROP USER db_cmd_user_stream;
SELECT PG_DROP_REPLICATION_SLOT('cmd_replicaton_test');
```

### Streaming Setup

- **Replication Slot Name**: `cmd_replicaton_test`
- **Publication Name**: `cmd_replicaton_test`

### Pour Data into DB
- read data from Bigquery Raw-dev
- Insert into PG
```Python
python3 test/insert_data_into_PG.py
```

Ensure all configurations are properly set up to ensure smooth data processing from Datastream to BigQuery.

For any issues or further questions, please reach me nhan.huynh@unifiedpost.com