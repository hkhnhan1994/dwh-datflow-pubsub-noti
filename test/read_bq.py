"""CCreate a table in prod using a sql query and impersonation."""

from google.cloud import bigquery
# import google.oauth2.credentials
# import googleapiclient.discovery
import io
import json
import psycopg2
import pandas as pd
# def get_credentials(service_account):
#     """
#     Return a credential from a service account.

#     :param service_account: the full name of the service account
#     :return: the credential to be used for authentication
#     """
#     iam = googleapiclient.discovery.build("iamcredentials", "v1")
#     token = (
#         iam.projects()
#         .serviceAccounts()
#         .generateAccessToken(
#             name=f"projects/-/serviceAccounts/{service_account}".format(
#                 service_account=service_account
#             ),
#             body={
#                 "lifetime": "600s",
#                 "scope": [
#                     "https://www.googleapis.com/auth/bigquery",
#                     "https://www.googleapis.com/auth/bigquery.insertdata",
#                     "https://www.googleapis.com/auth/cloud-platform",
#                     "https://www.googleapis.com/auth/devstorage.full_control",
#                     "https://www.googleapis.com/auth/cloudkms",
#                     "https://www.googleapis.com/auth/logging.admin",
#                     "https://www.googleapis.com/auth/monitoring",
#                 ],
#             },
#         )
#     )
#     token = token.execute()["accessToken"]
#     credentials = google.oauth2.credentials.Credentials(token)
#     return credentials


# # "sa-dw-bqmaintenance-dev@pj-bu-dw-orch-dev.iam.gserviceaccount.com"
# # "sa-dw-bqmaintenance-uat@pj-bu-dw-orch-uat.iam.gserviceaccount.com"
# # "sa-dw-bqmaintenance-prod@pj-bu-dw-orch-prod.iam.gserviceaccount.com"

# env = "dev"  # prod uat dev
# service_account = (
#     f"sa-dw-bqmaintenance-{env}@pj-bu-dw-orch-{env}.iam.gserviceaccount.com"
# )

project = "pj-bu-dw-raw-dev"
dataset = "P1_PCMD"
# table_id = "customers"




    
def read_bq(project,dataset,table_id,client):

    query_job = client.query(
        f"""select * from {project}.{dataset}.{table_id}"""
        ) 
    rows = query_job.result().to_dataframe()
    schema = client.get_table(f"{project}.{dataset}.{table_id}")
    f = io.StringIO("")
    client.schema_to_json(schema.schema,f)
    return rows , json.loads(f.getvalue()) # return data and schema

def convert_bq_schema_to_postgres(bigquery_schema):
    data_type_mapping = {
    "STRING": "TEXT",
    "BYTES": "BYTEA",
    "INTEGER": "INTEGER",
    "FLOAT": "DOUBLE PRECISION",
    "TIMESTAMP": "TIMESTAMP",
    "FLOAT": "REAL",
    "NUMERIC": "DECIMAL",
    "DATETIME": "SMALLDATETIME"
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
# Function to convert a value to PostgreSQL format
def to_pg_format(value):
    if pd.isna(value) or value == pd.NaT or value is None:  # This handles None and NaT
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, str):
        return value
    elif isinstance(value, pd.Timestamp):
        return value.isoformat()
    else:
        return str(value)


def create_table_insert_data_pg(data,schema,
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
    try:
        cursor = conn.cursor()

        # Create a PostgreSQL table if it doesn't exist
        create_table_query = f"""
        DROP TABLE IF EXISTS {pg_table};
        CREATE TABLE IF NOT EXISTS {pg_table} (
            {', '.join([f'"{col}" {type}' for col,type in schema.items()])}
        );
        """
        # print(create_table_query)
        cursor.execute(create_table_query)
        conn.commit()

        # Insert data into PostgreSQL
        counter = 0
        for index, row in data.iterrows():
            # Extract column names
            columns = ', '.join(data.columns)
            
            # Convert row values to PostgreSQL format
            # Convert row values to PostgreSQL format
            values = [to_pg_format(x) for x in row.values]

            # Create the INSERT query with placeholders
            insert_query = f"""
            INSERT INTO {pg_table} ({columns}) 
            VALUES ({', '.join(['%s'] * len(values))})
            """
            insert_query = insert_query.replace("'", '"')
            # Print the query for debugging purposes
            # print(insert_query)
            try:
                cursor.execute(insert_query, tuple(values))
                counter = counter+1
            except Exception as e:
                print(e)
                print(values)
                conn.rollback()
                continue
        print(f"inserted {counter} into table {pg_table}")
        # Commit the transaction and close the connection
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

def read_bq_to_postgres(
    project,
    dataset,
    client,
    pg_host='localhost',
    pg_port='5432',
    pg_dbname='db_cmd',
    pg_user='db_cmd_user',
    pg_password='123456'):
    tables = client.list_tables(dataset)
    for table in tables:
        data, schema = read_bq(project,dataset,table.table_id,client)
        postgres_schema = convert_bq_schema_to_postgres(schema)
        # print(postgres_schema)
        # data = data.where(pd.notnull(data), None)
        create_table_insert_data_pg(data,postgres_schema, pg_host,pg_port,pg_dbname,pg_user,pg_password,table.table_id)
        
        data = data.replace({pd.NA: None})

client = bigquery.Client(project=project)

read_bq_to_postgres(project,dataset,client)

# bq_data, bq_schema  = read_bq(project,dataset,table_id,client)
# postgres_schema = convert_bq_schema_to_postgres(bq_schema)
