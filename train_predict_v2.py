# dags/train_predict_v2.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

@task
def train(train_input_table, train_view, forecast_function_name):
    cur = return_snowflake_conn()
    
    cur.execute("CREATE SCHEMA IF NOT EXISTS USER_DB_PLATYPUS.ADHOC;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS USER_DB_PLATYPUS.ANALYTICS;")

    create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT DATE, CLOSE, SYMBOL
        FROM {train_input_table};
    """

    create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA        => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME    => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME    => 'CLOSE',
            CONFIG_OBJECT     => {{ 'ON_ERROR': 'SKIP' }}
        );
    """
    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    finally:
        try: cur.close()
        except Exception: pass


@task
def predict(forecast_function_name, train_input_table, forecast_table, final_table):
    """
    - Generate predictions and store them in forecast_table
    - Union predictions with historical data into final_table
    """
    cur = return_snowflake_conn()

    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{ 'prediction_interval': 0.95 }}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS
            SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual,
               NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT REPLACE(series, '\"', '') AS SYMBOL,
               ts AS DATE, NULL AS actual,
               forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    finally:
        try: cur.close()
        except Exception: pass

with DAG(
    dag_id='lab_forecast_dag',
    start_date=datetime(2025, 9, 29),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule=None,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    train_input_table      = "USER_DB_PLATYPUS.RAW.TWO_STOCK_V2"
    train_view             = "USER_DB_PLATYPUS.ADHOC.TWO_STOCK_VIEW"
    forecast_table         = "USER_DB_PLATYPUS.ADHOC.TWO_STOCK_FORECAST"
    forecast_function_name = "USER_DB_PLATYPUS.ANALYTICS.PREDICT_TWO_STOCK_PRICE"
    final_table            = "USER_DB_PLATYPUS.ANALYTICS.TWO_STOCK"

    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)
    train_task >> predict_task
