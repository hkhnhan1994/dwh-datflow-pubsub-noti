"""CCreate a table in prod using a sql query and impersonation."""

from google.cloud import bigquery
import google.oauth2.credentials
import googleapiclient.discovery
import io
import json
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from tqdm import tqdm   
import google.oauth2.credentials
import googleapiclient.discovery
import datetime
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed
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
dataset = [
        #    "P1_PCMD",
        #    "P1_PACI",
        #    "H1_HEHE",
        #    "H2_HEHE",
	    "H3_HEHE",
        #    "H1_HKLC",
        #    "H2_HKLC",
        "H3_HKLC",
        "H1_HKVK",
        # "D1_DDEL"
           ]
# table_id = "customers"
    
def read_bq(project,dataset,table_id,client, data_limit):
    print(f"reading data from GCP table {project}.{dataset}.{table_id}")
    query_job = client.query(
        f"""select * from {project}.{dataset}.{table_id} limit {data_limit}"""
        ) 
    rows = query_job.result().to_dataframe()
    # print(f"converted to df")
    schema = client.get_table(f"{project}.{dataset}.{table_id}")
    f = io.StringIO("")
    client.schema_to_json(schema.schema,f)
    # print('get schema')
    dir = pathlib.Path(f'test/.source/{data_limit}/{table_id}')
    dir.mkdir(parents=True,exist_ok=True)
    rows.to_json(dir/'data.json')
    client.schema_to_json(schema.schema,f'test/.source/{data_limit}/{table_id}/schema.json')
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
    "DATETIME": "TIMESTAMP",
    "BOOLEAN": "BOOLEAN"
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
        postgres_schema.update({col.lower():data_type})
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
    elif isinstance(value, (pd.Timestamp, datetime.datetime)):
        return value.isoformat()
    else:
        return str(value)


def create_table_insert_data_pg(data, schema,
    pg_host='your_postgres_host',
    pg_port='your_postgres_port',
    pg_dbname='your_postgres_dbname',
    pg_user='your_postgres_username',
    pg_password='your_postgres_password',
    pg_table='your_postgres_table',
    batch_size=5000
):
    """
    Create a PostgreSQL table based on schema and insert data efficiently.
    Uses psycopg2.extras.execute_values for batch inserts.
    """

    # Establish connection to PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_dbname,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    try:
        # --- Create table ---
        schema.update({'extra_col1': 'TIMESTAMP'})  # test schema change
        create_table_query = f"""
        DROP TABLE IF EXISTS "{pg_table}";
        CREATE TABLE IF NOT EXISTS "{pg_table}" (
            {', '.join([f'"{col}" {col_type}' for col, col_type in schema.items()])}
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # --- Prepare data ---
        columns = ', '.join(data.columns).lower() + ', extra_col1'
        all_values = []

        for _, row in tqdm(data.iterrows(), total=data.shape[0], desc=f"table {pg_table}"):
            values = []
            for col_name, value in zip(data.columns, row.values):
                pg_type = schema.get(col_name.lower())

                # Handle NaN/None
                if pd.isna(value):
                    values.append(None)
                    continue

                # Handle timestamps
                if pg_type and 'timestamp' in pg_type.lower() and isinstance(value, (int, float)):
                    try:
                        value = datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
                    except (OverflowError, ValueError):
                        value = None

                # Handle booleans
                if pg_type and 'boolean' in pg_type.lower() and isinstance(value, (int, float)):
                    if value == 0:
                        value = False
                    elif value == 1:
                        value = True
                    else:
                        value = None

                values.append(value)

            # Add extra column
            values.append(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
            all_values.append(tuple(values))

        # --- Insert data in batches ---
        insert_query = f"""
        INSERT INTO "{pg_table}" ({columns}) VALUES %s
        """
        for i in range(0, len(all_values), batch_size):
            batch = all_values[i:i+batch_size]
            execute_values(cursor, insert_query, batch, page_size=batch_size)
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    return {'table name': pg_table, 'Number records': data.shape[0]}


def read_bq_to_postgres(
    project,
    datasets,
    client):
    for dataset in datasets:
        tables = client.list_tables(dataset)
        for table in tables:
            read_bq(project,dataset,table.table_id,client, 500000)
def process_folder(folder, pg_host, pg_port, pg_dbname, pg_user, pg_password):
    """Helper: process one folder -> read data + schema -> insert to Postgres."""
    data_file = folder / "data.json"
    schema_file = folder / "schema.json"
    if not (data_file.exists() and schema_file.exists()):
        return None

    with open(data_file, "r") as f:
        data = pd.read_json(f)
    with open(schema_file, "r") as f:
        schema = json.load(f)

    postgres_schema = convert_bq_schema_to_postgres(schema)
    data = data.replace({pd.NA: None})

    new_record = create_table_insert_data_pg(
        data,
        postgres_schema,
        pg_host,
        pg_port,
        pg_dbname,
        pg_user,
        pg_password,
        folder.name
    )
    return new_record


def insert_data_to_postgres(
    data_number,
    pg_host="35.241.187.220",
    pg_port="5432",
    pg_dbname="postgres",
    pg_user="postgres",
    pg_password="1$k_oY<K#n)DivT-",
    max_workers=8
):
    base_dir = pathlib.Path(f"test/.source/{data_number}")
    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_folder = {
            executor.submit(
                process_folder, folder, pg_host, pg_port, pg_dbname, pg_user, pg_password
            ): folder
            for folder in base_dir.iterdir()
            if folder.is_dir()
        }

        for future in as_completed(future_to_folder):
            folder = future_to_folder[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                print(f"❌ Error processing {folder.name}: {e}")

    export_table = pd.DataFrame(results)
    return export_table
client = bigquery.Client(project=project)
if __name__ == "__main__":
    # read_bq_to_postgres(project,dataset,client)
    insert_data_to_postgres(100000)