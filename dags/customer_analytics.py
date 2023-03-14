from datetime import datetime 

from astro import sql as aql 
from astro.files import File 
from astro.sql.table import Table 
from airflow.decorators import dag, task, task_group
from cosmos.providers.dbt.task_group import DbtTaskGroup
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
            snowflake_table = Table(name=f'STG_{source}',conn_id=_SNOWFLAKE_CONN,temp=False)
            extract_source(source, output_table=snowflake_table)
            
        output_tables.append(snowflake_table)
                
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
    
    @aql.dataframe()
    def feature_engineering(transformed_data, customer_df:pd.DataFrame, churned_df:pd.DataFrame):
    
        customer_df['CUSTOMER_ID'] = customer_df['CUSTOMER_ID'].apply(str)
        customer_df.set_index('CUSTOMER_ID', inplace=True)

        churned_df['CUSTOMER_ID'] = churned_df['CUSTOMER_ID'].apply(str)
        churned_df.set_index('CUSTOMER_ID', inplace=True)
        churned_df['IS_ACTIVE'] = churned_df['IS_ACTIVE'].astype(int).replace(0, 1)

        df = customer_df[['NUMBER_OF_ORDERS', 'CUSTOMER_LIFETIME_VALUE']].join(churned_df[['IS_ACTIVE']], how='left').fillna(0)

        return df.reset_index(inplace=True)
        
    @aql.dataframe()
    def train_churn(wandb_project:str, df:pd.DataFrame) -> dict:

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

        customer_df['CUSTOMER_ID'] = customer_df['CUSTOMER_ID'].apply(str)
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

        return customer_df

    _extract_and_load_structured_data = extract_and_load_structured_data(sources)
    _transform_structured_data = transform_structured_data(_extract_and_load_structured_data)
    _feature_engineering = feature_engineering(
        _transform_structured_data,
        customer_df=Table(name="CUSTOMERS", conn_id=_SNOWFLAKE_CONN), 
        churned_df=Table(name="CUSTOMER_CHURN_MONTH", conn_id=_SNOWFLAKE_CONN)
    )
    _model_info = train_churn(wandb_project='demo', df=_feature_engineering)
    _predict_churn = predict_churn(_model_info, customer_df=Table(name="CUSTOMERS", conn_id=_SNOWFLAKE_CONN))
    
    chain(_extract_and_load_structured_data >> _transform_structured_data >> _feature_engineering)

customer_analytics()

# from include.helpers import cleanup_snowflake
# cleanup_snowflake(database='', schema='')