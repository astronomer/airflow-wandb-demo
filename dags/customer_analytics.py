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

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split 
from sklearn.ensemble import RandomForestClassifier
import tempfile
import pickle
import wandb
from wandb.sklearn import plot_precision_recall, plot_feature_importances
from wandb.sklearn import plot_class_proportions, plot_learning_curve, plot_roc

_SNOWFLAKE_CONN = "snowflake_default"

@dag(schedule_interval=None, start_date=datetime(2023, 1, 1), catchup=False, )
def customer_analytics():

    # raw_data_bucket = 'raw-data'
    local_data_dir = 'include/data'
    sources = ['customers', 'payments', 'subscription_periods', 'customer_conversions', 'orders', 'sessions', 'ad_spend']
    
    @task_group()
    def extract_and_load_structured_data(sources):
        output_tables=[]
        for source in sources:
            @aql.dataframe(task_id=f'extract_load_{source}')
            def extract_source(source):
                return pd.read_csv(f'{local_data_dir}/{source}.csv')

        output_tables.append(Table(name=source,conn_id=_SNOWFLAKE_CONN,temp=False))
        extract_source(source)
        
        return output_tables

    # @task_group()
    # def extract_structured_data():
    #     @task()
    #     def extract_SFDC_data():
    #         s3hook = S3Hook()
    #         for source in sfdc_sources:    
    #             s3hook.load_file(
    #                 filename=f'{local_data_dir}/{source}.csv', 
    #                 key=f'{source}.csv', 
    #                 bucket_name=raw_data_bucket,
    #                 replace=True,
    #             )
        
    #     @task()
    #     def extract_hubspot_data():
    #         s3hook = S3Hook()
    #         for source in hubspot_sources:    
    #             s3hook.load_file(
    #                 filename=f'{local_data_dir}/{source}.csv', 
    #                 key=f'{source}.csv', 
    #                 bucket_name=raw_data_bucket,
    #                 replace=True,
    #             )

    #     @task()
    #     def extract_segment_data():
    #         s3hook = S3Hook()
    #         for source in segment_sources:    
    #             s3hook.load_file(
    #                 filename=f'{local_data_dir}/{source}.csv', 
    #                 key=f'{source}.csv', 
    #                 bucket_name=raw_data_bucket,
    #                 replace=True,
    #             )
        
    #     [extract_SFDC_data(), extract_hubspot_data(), extract_segment_data()]

    # @task_group()
    # def load_structured_data():
    #     for source in sfdc_sources:
    #         aql.load_file(task_id=f'load_{source}',
    #             input_file = File(f"S3://{raw_data_bucket}/{source}.csv"), 
    #             output_table = Table(name=f'STG_{source.upper()}', conn_id=_SNOWFLAKE_CONN)
    #         )
        
    #     for source in hubspot_sources:
    #         aql.load_file(task_id=f'load_{source}',
    #             input_file = File(f"S3://{raw_data_bucket}/{source}.csv"), 
    #             output_table = Table(name=f'STG_{source.upper()}', conn_id=_SNOWFLAKE_CONN)
    #         )
        
    #     for source in segment_sources:
    #         aql.load_file(task_id=f'load_{source}',
    #             input_file = File(f"S3://{raw_data_bucket}/{source}.csv"), 
    #             output_table = Table(name=f'STG_{source.upper()}', conn_id=_SNOWFLAKE_CONN)
    #         )
        
    @task_group()
    def transform_structured_data(loaded_data):
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
    
        return 'success'

    # Table(name="CUSTOMERS", conn_id=_SNOWFLAKE_CONN)
    # Table(name="CUSTOMER_CHURN_MONTH", conn_id=_SNOWFLAKE_CONN)
    

    # @aql.transform(conn_id=_SNOWFLAKE_CONN)
    # def get_customers():
    #     return 'SELECT CUSTOMER_ID, NUMBER_OF_ORDERS, CUSTOMER_LIFETIME_VALUE FROM CUSTOMERS;'

    # @aql.transform(conn_id=_SNOWFLAKE_CONN)
    #     def get_churn():
    #         return 'SELECT CUSTOMER_ID, IS_ACTIVE FROM CUSTOMER_CHURN_MONTH;'

    @aql.dataframe()
    def feature_engineering(transformed_data, customer_df:pd.DataFrame, churned_df:pd.DataFrame):
        # from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        # snowflake_hook = SnowflakeHook()

        # customer_df = snowflake_hook.get_pandas_df('SELECT * FROM CUSTOMERS;')
        
        customer_df['CUSTOMER_ID'] = customer_df['CUSTOMER_ID'].apply(str)
        customer_df.set_index('CUSTOMER_ID', inplace=True)

        # churned_df = snowflake_hook.get_pandas_df('SELECT * FROM CUSTOMER_CHURN_MONTH;') 
        churned_df['CUSTOMER_ID'] = churned_df['CUSTOMER_ID'].apply(str)
        churned_df.set_index('CUSTOMER_ID', inplace=True)
        churned_df['IS_ACTIVE'] = churned_df['IS_ACTIVE'].astype(int).replace(0, 1)

        df = customer_df[['NUMBER_OF_ORDERS', 'CUSTOMER_LIFETIME_VALUE']].join(churned_df[['IS_ACTIVE']], how='left').fillna(0)

        return df.reset_index(inplace=True)
        
    @aql.dataframe()
    def train_churn(wandb_project:str, df:pd.DataFrame) -> dict:
        

        # from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        # snowflake_hook = SnowflakeHook()

        # customer_df = snowflake_hook.get_pandas_df('SELECT CUSTOMER_ID, NUMBER_OF_ORDERS, CUSTOMER_LIFETIME_VALUE FROM CUSTOMERS;')
        # customer_df['CUSTOMER_ID'] = customer_df['CUSTOMER_ID'].apply(str)
        # customer_df.set_index('CUSTOMER_ID', inplace=True)

        # churned_df = snowflake_hook.get_pandas_df('SELECT CUSTOMER_ID, IS_ACTIVE FROM CUSTOMER_CHURN_MONTH;') 
        # churned_df['CUSTOMER_ID'] = churned_df['CUSTOMER_ID'].apply(str)
        # churned_df.set_index('CUSTOMER_ID', inplace=True)
        # churned_df['IS_ACTIVE'] = churned_df['IS_ACTIVE'].astype(int).replace(0, 1)
        
        # df = customer_df.join(churned_df, how='left').fillna(0)

        features = ['NUMBER_OF_ORDERS', 'CUSTOMER_LIFETIME_VALUE']
        target = ['IS_ACTIVE']

        test_size=.3
        X_train, X_test, y_train, y_test = train_test_split(df[features], df[target], test_size=test_size, random_state=1883)
        X_train = np.array(X_train.values.tolist())
        y_train = np.array(y_train.values.tolist()).reshape(len(y_train),)
        y_train = y_train.reshape(len(y_train),)
        X_test = np.array(X_test.values.tolist())
        y_test = np.array(y_test.values.tolist())
        y_test = y_test.reshape(len(y_test),)
        
        model = RandomForestClassifier()
        _ = model.fit(X_train, y_train)
        model_params = model.get_params()

        y_pred = model.predict(X_test)
        y_probas = model.predict_proba(X_test)
        importances = model.feature_importances_
        indices = np.argsort(importances)[::-1]

        wandb.login()
        run = wandb.init(
            project=wandb_project, 
            config=model_params, 
            group='wandb-demo', 
            name='jaffle_churn', 
            dir='include',
            mode='online')

        wandb.config.update(
            {
                "test_size" : test_size,
                "train_len" : len(X_train),
                "test_len" : len(X_test)
            }
        )
        plot_class_proportions(y_train, y_test, ['not_churned','churned'])
        plot_learning_curve(model, X_train, y_train)
        plot_roc(y_test, y_probas, ['not_churned','churned'])
        plot_precision_recall(y_test, y_probas, ['not_churned','churned'])
        plot_feature_importances(model)

        model_artifact_name = 'churn_classifier'

        with tempfile.NamedTemporaryFile() as tf:
            pickle.dump(model, tf)
            artifact = wandb.Artifact(model_artifact_name, type='model')
            artifact.add_file(local_path=tf.name, name=model_artifact_name)
            wandb.log_artifact(artifact)

        wandb.finish()

        return {'wandb_project':wandb_project, 'run_id':run.id, 'artifact_name':model_artifact_name}

    @aql.dataframe()
    def predict_churn(model_info:dict, customer_df:pd.DataFrame):
        
        # from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        # from snowflake.connector.pandas_tools import write_pandas

        # pred_table_name = 'PRED_CUSTOMER_CHURN'

        # snowflake_hook = SnowflakeHook()

        # customer_df = snowflake_hook.get_pandas_df('SELECT * FROM CUSTOMERS;')
        customer_df['CUSTOMER_ID'] = customer_df['CUSTOMER_ID'].apply(str)
        # customer_df.set_index('CUSTOMER_ID', inplace=True)
        customer_df.fillna(0, inplace=True)
        
        features = ['NUMBER_OF_ORDERS', 'CUSTOMER_LIFETIME_VALUE']

        wandb.login()
        run = wandb.init(
            project=model_info['wandb_project'], 
            group='wandb-demo', 
            name='jaffle_churn', 
            dir='include',
            resume='must',
            id=model_info['run_id'])

        artifact = run.use_artifact(f"{model_info['artifact_name']}:latest", type='model')

        with tempfile.TemporaryDirectory() as td:
            model_file = artifact.file(td)
            with open(model_file, 'rb') as mf:
                model = pickle.load(mf)
                customer_df['PRED'] = model.predict_proba(np.array(customer_df[features].values.tolist()))[:,0]

        wandb.finish()

        customer_df.reset_index(inplace=True)

        # snowflake_hook.run(f'CREATE OR REPLACE TABLE {pred_table_name} (CUSTOMER_ID varchar(36), \
        #                                                         NUMBER_OF_ORDERS float, \
        #                                                         CUSTOMER_LIFETIME_VALUE float, \
        #                                                         PRED float);')

        # write_pandas(
        #     snowflake_hook.get_conn(), 
        #     customer_df[['CUSTOMER_ID', 'NUMBER_OF_ORDERS', 'CUSTOMER_LIFETIME_VALUE', 'PRED']], 
        #     'PRED_CUSTOMER_CHURN')

        return customer_df


    # _extract_structured_data = extract_structured_data()
    # _load_structured_data = load_structured_data()
    _extract_and_load_structured_data = extract_and_load_structured_data(sources)
    _transform_structured_data = transform_structured_data(_extract_and_load_structured_data)
    _feature_engineering = feature_engineering(
        _transform_structured_data,
        customer_df=Table(name="CUSTOMERS", conn_id=_SNOWFLAKE_CONN), 
        churned_df=Table(name="CUSTOMER_CHURN_MONTH", conn_id=_SNOWFLAKE_CONN)
    )
    _model_info = train_churn(wandb_project='demo', df=_feature_engineering)
    _predict_churn = predict_churn(_model_info, customer_df=Table(name="CUSTOMERS", conn_id=_SNOWFLAKE_CONN))
    
    # _extract_structured_data >> _load_structured_data >> _transform_structured_data >> _train_churn >> _predict_churn >> aql.cleanup()

customer_analytics()

# from include.helpers import cleanup_snowflake
# cleanup_snowflake(database='', schema='')