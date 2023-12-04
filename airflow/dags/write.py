import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonVirtualenvOperator
from datetime import timedelta

def write():
    import pandas as pd
    import logging
    import redis
    import json
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from google.cloud.exceptions import NotFound

    log = logging.getLogger(__name__)

    # Function to convert bytes to str
    def btod(b):
        return b.decode("utf-8")
    
    # Try to connect to Redis
    try:
        r = redis.Redis(host='redis-cours', port=6379, db=0)
        r.ping() 
        log.info("Success to connect to Redis")
    except redis.ConnectionError as e:
        log.error(f"Erreur de connexion à Redis : {e}")
    
    # Retrieve keys from Redis
    posts_keys = r.keys()
    posts_by_user = {}

    for key in posts_keys:
        post_redis = r.get(btod(key))
        post_str = btod(post_redis)
        post_dict = json.loads(post_str)
        
        if post_dict.get("@OwnerUserId") is not None:
            user_id = post_dict["@OwnerUserId"]
            if posts_by_user.get(user_id) is not None:
                posts_by_user[user_id] += 1
            else:
                posts_by_user[user_id] = 1

        
    # Initialize BigQuery Client
    key_path = "./data/movies-stackexchange/json/service-account.json"
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


    # Add data to Big Query
    df = pd.DataFrame(list(posts_by_user.items()), columns=['user_id', 'count'])
    table = 'movieStackExchange.postCount'

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Load data to BigQuery table
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()

    log.info("Data successfully loaded into BigQuery.")

with DAG(
    dag_id="DAG_write_to_bigQuery",
    schedule=timedelta(minutes=30),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    
    virtual_classic = PythonVirtualenvOperator(
        task_id="write",
        requirements=["google-cloud-bigquery"],
        python_callable=write,
    )