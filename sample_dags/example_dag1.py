import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

from atlan_lite.models.assets import SnowflakeTable, File

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="example_dag1", default_args=args, schedule_interval=None
)

table_name = "a_test_table"

create_insert_query = [
    """DROP TABLE IF EXISTS "public.{}";""".format(table_name), 
    """create table public.{} (amount number);""".format(table_name),
    """insert into public.{} values(1),(2),(3);""".format(table_name),
]

def row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_demo_db")
    result = dwh_hook.get_first("select count(*) from public.{}".format(table_name))
    logging.info("Number of rows in `public.%s`  - %s", table_name, result[0])

f = File("/tmp/input/{{ execution_date }}")

with dag:
    create = SnowflakeOperator(
        task_id="create", 
        sql=create_insert_query,
        snowflake_conn_id="snowflake_demo_db",
        outlets={"datasets": [SnowflakeTable(table_alias="cy25812.ap-southeast-1/demo_db/public/{}".format(table_name), name = table_name)]}
    )

    get_count = PythonOperator(
        task_id="get_count", 
        python_callable=row_count,
        inlets={"datasets": [SnowflakeTable(table_alias="cy25812.ap-southeast-1/demo_db/public/{}".format(table_name), name = table_name)]}, 
        outlets={"datasets":[f]}
    )
    

create >> get_count
