"""CCreate a table in prod using a sql query and impersonation."""

from google.cloud import bigquery
import google.oauth2.credentials
import googleapiclient.discovery
import io
import json
import psycopg2
import pandas as pd
def get_credentials(service_account):
    """
    Return a credential from a service account.

    :param service_account: the full name of the service account
    :return: the credential to be used for authentication
    """
    iam = googleapiclient.discovery.build("iamcredentials", "v1")
    token = (
        iam.projects()
        .serviceAccounts()
        .generateAccessToken(
            name=f"projects/-/serviceAccounts/{service_account}".format(
                service_account=service_account
            ),
            body={
                "lifetime": "600s",
                "scope": [
                    "https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/bigquery.insertdata",
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/devstorage.full_control",
                    "https://www.googleapis.com/auth/cloudkms",
                    "https://www.googleapis.com/auth/logging.admin",
                    "https://www.googleapis.com/auth/monitoring",
                ],
            },
        )
    )
    token = token.execute()["accessToken"]
    credentials = google.oauth2.credentials.Credentials(token)
    return credentials


# "sa-dw-bqmaintenance-dev@pj-bu-dw-orch-dev.iam.gserviceaccount.com"
# "sa-dw-bqmaintenance-uat@pj-bu-dw-orch-uat.iam.gserviceaccount.com"
# "sa-dw-bqmaintenance-prod@pj-bu-dw-orch-prod.iam.gserviceaccount.com"

env = "dev"  # prod uat dev
service_account = (
    f"sa-dw-bqmaintenance-{env}@pj-bu-dw-orch-{env}.iam.gserviceaccount.com"
)

project = "pj-bu-dw-raw-dev"
dataset = "B1_BCOM"
table_id = "customers"




    
def read_bq(project,dataset,table_id,client):

    query_job = client.query(
        f"""select * from {project}.{dataset}.{table_id} limit 10"""
        ) 
    rows = query_job.result().to_dataframe()
    # records = [dict(row) for row in rows]
    # json_obj = json.dumps(str(records))
    # schema = client.get_table(f"{project}.{dataset}.{table_id}")
    # f = io.StringIO("")
    # client.schema_to_json(schema.schema,f)
    return rows #, json.loads(f.getvalue()) # return data and schema

def convert_bq_schema_to_postgres(bigquery_schema):
    data_type_mapping = {
    "STRING": "TEXT",
    "BYTES": "BYTEA",
    "INTEGER": "INTEGER",
    "FLOAT": "DOUBLE PRECISION",
    # ... add more mappings as needed
    }
    postgres_schema = {}
    def convert_field(field):
        
        postgres_type = data_type_mapping.get(field['type'], "TEXT")  # Default to TEXT
        if field['mode'] == "REPEATED":
            postgres_type = f"{postgres_type}[]"  # Array type
        elif field['mode'] == "REQUIRED":
            postgres_type = f"{postgres_type} NOT NULL"
        return field['name'],postgres_type
    for field in bigquery_schema:
        col, data_type = convert_field(field)
        postgres_schema.update({col:data_type})
    return postgres_schema


def to_postgres(data, 
    pg_host='your_postgres_host',
    pg_port='your_postgres_port',
    pg_dbname='your_postgres_dbname',
    pg_user='your_postgres_username',
    pg_password='your_postgres_password',
    pg_table='your_postgres_table'):
    # Establish connection to PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_dbname,
        user=pg_user,
        password=pg_password
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Create a PostgreSQL table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {pg_table} (
        {', '.join([f'{col} TEXT' for col in data.columns])}
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into PostgreSQL
    for index, row in data.iterrows():
        insert_query = f"""
        INSERT INTO {pg_table} ({', '.join(data.columns)}) 
        VALUES ({', '.join(['%s'] * len(row))})
        """
        cursor.execute(insert_query, tuple(row))

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def read_bq_to_postgres(
    project,
    dataset,
    client,
    pg_host='localhost',
    pg_port='5432',
    pg_dbname='cmd_db',
    pg_user='datastream_user',
    pg_password='123456'):
    tables = client.list_tables(dataset)
    for table in tables:
        data = read_bq(project,dataset,table.table_id,client)
        to_postgres(data, pg_host,pg_port,pg_dbname,pg_user,pg_password,table.table_id)

# bq_data, bq_schema  = read_bq(project,dataset,table_id,service_account)
# postgres_schema = convert_bq_schema_to_postgres(bq_schema)
# print(bq_data)
credentials = get_credentials(service_account)
client = bigquery.Client(project=project,credentials=credentials)
read_bq_to_postgres(project,dataset,client)