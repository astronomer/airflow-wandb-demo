from datetime import datetime 
import os
from pathlib import Path
import requests

from astro import sql as aql 
from astro.files import File 
from astro.sql.table import Table 
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from cosmos.providers.dbt.task_group import DbtTaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.helpers import chain


_SNOWFLAKE_CONN = "snowflake_default"

@dag(schedule_interval=None, start_date=datetime(2023, 1, 1), catchup=False, )
def customer_analytics():

    raw_data_bucket = 'raw-data'
    local_data_dir = 'include/data'
    hubspot_sources = ['ad_spend']
    segment_sources = ['sessions']
    sfdc_sources = ['customers', 'payments', 'subscription_periods', 'customer_conversions', 'orders']
    # minio_endpoint = "http://host.docker.internal:9000" #TODO: replace with service discovery
    
    @task_group()
    def extract_structured_data():
        @task()
        def extract_SFDC_data():
            s3hook = S3Hook()
            for source in sfdc_sources:    
                s3hook.load_file(
                    filename=f'{local_data_dir}/{source}.csv', 
                    key=f'{source}.csv', 
                    bucket_name=raw_data_bucket,
                    replace=True,
                )
        
        @task()
        def extract_hubspot_data():
            s3hook = S3Hook()
            for source in hubspot_sources:    
                s3hook.load_file(
                    filename=f'{local_data_dir}/{source}.csv', 
                    key=f'{source}.csv', 
                    bucket_name=raw_data_bucket,
                    replace=True,
                )

        @task()
        def extract_segment_data():
            s3hook = S3Hook()
            for source in segment_sources:    
                s3hook.load_file(
                    filename=f'{local_data_dir}/{source}.csv', 
                    key=f'{source}.csv', 
                    bucket_name=raw_data_bucket,
                    replace=True,
                )
        
        [extract_SFDC_data(), extract_hubspot_data(), extract_segment_data()]

    @task_group()
    def load_structured_data():
        for source in sfdc_sources:
            aql.load_file(task_id=f'load_{source}',
                input_file = File(f"S3://{raw_data_bucket}/{source}.csv"), 
                output_table = Table(name=f'STG_{source.upper()}', conn_id=_SNOWFLAKE_CONN)
            )
        
        for source in hubspot_sources:
            aql.load_file(task_id=f'load_{source}',
                input_file = File(f"S3://{raw_data_bucket}/{source}.csv"), 
                output_table = Table(name=f'STG_{source.upper()}', conn_id=_SNOWFLAKE_CONN)
            )
        
        for source in segment_sources:
            aql.load_file(task_id=f'load_{source}',
                input_file = File(f"S3://{raw_data_bucket}/{source}.csv"), 
                output_table = Table(name=f'STG_{source.upper()}', conn_id=_SNOWFLAKE_CONN)
            )
        
    @task_group()
    def transform_structured_data():
        jaffle_shop = DbtTaskGroup(
            dbt_project_name="jaffle_shop",
            dbt_root_path="/usr/local/airflow/include/dbt",
            conn_id=_SNOWFLAKE_CONN,
            dbt_args={"dbt_executable_path": "/home/astro/.venv/dbt/bin/dbt"},
            test_behavior="after_all",
        )
        
        attribution_playbook = DbtTaskGroup(
            dbt_project_name="attribution_playbook",
            dbt_root_path="/usr/local/airflow/include/dbt",
            conn_id=_SNOWFLAKE_CONN,
            dbt_args={"dbt_executable_path": "/home/astro/.venv/dbt/bin/dbt"},
        )

        mrr_playbook = DbtTaskGroup(
            dbt_project_name="mrr_playbook",
            dbt_root_path="/usr/local/airflow/include/dbt",
            conn_id=_SNOWFLAKE_CONN,
            dbt_args={"dbt_executable_path": "/home/astro/.venv/dbt/bin/dbt"},
        )

        
    @task()
    def train_churn() -> str:

        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        import pandas as pd
        import numpy as np
        from sklearn.model_selection import train_test_split 
        import mlflow
        from mlflow.keras import log_model
        from keras.models import Sequential
        from keras import layers
        import weaviate
        import os

        hook = SnowflakeHook()
        
        os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
        os.environ['MLFLOW_S3_ENDPOINT_URL'] = mlflow_s3_endpoint_url
        os.environ['MLFLOW_TRACKING_URI'] = mlflow_tracking_uri

        df = hook.get_pandas_df('SELECT * FROM COMMENT_TRAINING;') 
        df['LABEL'] = df['LABEL'].apply(int)
        df = df.replace(r'^\s*$', np.nan, regex=True).dropna()
        df['REVIEW_TEXT'] = df['REVIEW_TEXT'].apply(lambda x: x.replace("\n",""))
        
        #read from pre-existing embeddings in weaviate
        client = weaviate.Client(url = weaviate_endpoint)
        df['VECTOR'] = df.apply(lambda x: client.data_object.get(class_name='CommentTraining', uuid=x.UUID, with_vector=True)['vector'], axis=1)

        with mlflow.start_run(run_name='keras_pretrained_embeddings') as run:

            X_train, X_test, y_train, y_test = train_test_split(df['VECTOR'], df['LABEL'], test_size=.3, random_state=1883)
            X_train = np.array(X_train.values.tolist())
            y_train = np.array(y_train.values.tolist())
            X_test = np.array(X_test.values.tolist())
            y_test = np.array(y_test.values.tolist())
            
            model = Sequential()
            model.add(layers.Dense(1, activation='sigmoid'))
            model.compile(optimizer='rmsprop',loss='binary_crossentropy', metrics=['accuracy'])
            model.fit(X_train, y_train, epochs=70,validation_data=(X_test, y_test))

            mflow_model_info = log_model(model=model, artifact_path='sentiment_classifier')
            model_uri = mflow_model_info.model_uri

        return model_uri

    @task()
    def call_sentiment(
        weaviate_endpoint:str,  
        mlflow_tracking_uri:str,
        mlflow_s3_endpoint_url:str, 
        aws_access_key_id:str, 
        aws_secret_access_key:str,
        model_uri:str
    ):
        from mlflow.keras import load_model
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        import weaviate
        import numpy as np
        from snowflake.connector.pandas_tools import write_pandas
        import os

        os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = mlflow_s3_endpoint_url
        os.environ['MLFLOW_TRACKING_URI'] = mlflow_tracking_uri

        snowflake_hook = SnowflakeHook()
        df = snowflake_hook.get_pandas_df('SELECT * FROM CUSTOMER_CALLS;') 

        client = weaviate.Client(url = weaviate_endpoint)
        df['VECTOR'] = df.apply(lambda x: client.data_object.get(class_name='CustomerCall', uuid=x.UUID, with_vector=True)['vector'], axis=1)

        model = load_model(model_uri=model_uri)
        
        df['SENTIMENT'] = model.predict(np.stack(df['VECTOR'].values))

        longest_text = df['TRANSCRIPT'].apply(len).max()

        snowflake_hook.run(f'CREATE OR REPLACE TABLE PRED_CUSTOMER_CALLS (CUSTOMER_ID varchar(36), \
                                                                DATE date, \
                                                                RELATIVE_PATH varchar(20), \
                                                                TRANSCRIPT varchar({longest_text}), \
                                                                UUID varchar(36),\
                                                                SENTIMENT float);')

        write_pandas(
            snowflake_hook.get_conn(), 
            df[['CUSTOMER_ID', 'DATE', 'RELATIVE_PATH', 'TRANSCRIPT', 'UUID', 'SENTIMENT']], 
            'PRED_CUSTOMER_CALLS')

        return 'PRED_CUSTOMER_CALLS'

    _extract_structured_data = extract_structured_data()
    _load_structured_data = load_structured_data()
    _transform_structured_data = transform_structured_data()
    
    _extract_structured_data >> _load_structured_data >> _transform_structured_data >> aql.cleanup()

customer_analytics()

# from include.helpers import cleanup_snowflake
# cleanup_snowflake(database='', schema='')