from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def cleanup_snowflake(database:str, schema:str, snowflake_conn_id = 'snowflake_default', drop_list = ['PRED_', '_TMP_', 'STG_', 'TWITTER_', 'XCOM_', 'CUSTOMER_', 'COMMENT_']):
    hook = SnowflakeHook(snowflake_conn_id)
    hook.database=database
    hook.schema=schema

    stages = hook.get_records('SHOW STAGES')
    stages = [stage_name[1] for stage_name in stages]
    tables = hook.get_records('SHOW TABLES')
    tables = [table_name[1] for table_name in tables]
    views = hook.get_records('SHOW VIEWS')
    views = [view_name[1] for view_name in views]

    drop_tables =  [drop_table for drop_table in tables if any(drop_table for j in drop_list if str(j) in drop_table)]
    
    for table in drop_tables:
        hook.run(f'DROP TABLE IF EXISTS {table}')
        print(f'dropped table {database}.{schema}{table}')

    for stage in stages:
        hook.run(f'DROP STAGE IF EXISTS {stage}')
        print(f'dropped stage: {database}.{schema}.{stage}')

    for view in views:
        hook.run(f'DROP VIEW IF EXISTS {view}')
        print(f'dropped view: {database}.{schema}.{view}')

    return 

