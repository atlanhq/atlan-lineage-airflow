import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

from atlan.lineage.assets import SnowflakeTable, File

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="example_dag2", default_args=args, schedule_interval=None
)

create_insert_query = [
    """DROP TABLE IF EXISTS "public.{{ execution_date }}";""", 
    """create table "public.{{ execution_date }}" (amount number);""",
    """insert into "public.{{ execution_date }}" values(1),(2),(3);""",
]

def row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_demo_db")
    result = dwh_hook.get_first('select count(*) from PUBLIC."TABLE"')
    logging.info("Number of rows in `public.table`  - %s", result[0])

f = File("/tmp/output")

with dag:
    create = SnowflakeOperator(
        task_id="create", 
        sql=create_insert_query,
        snowflake_conn_id="snowflake_demo_db",
        outlets={"datasets": [SnowflakeTable(table_alias="cy25812.ap-southeast-1/demo_db/public/{{ execution_date }}", name = "{{ execution_date }}")]}
    )

    get_count = PythonOperator(
        task_id="get_count", 
        python_callable=row_count,
        inlets={"auto": True},
        outlets={"datasets":[f]}
    )

    query = SnowflakeOperator(
        task_id="query_snowfalke",
        sql="select count(*) from TPCH_SF10.REGION",
        snowflake_conn_id="snowflake_conn",
        inlets={"task_ids": ["create", "get_count"]},
        outlets={"datasets":[SnowflakeTable(table_alias="cy25812.ap-southeast-1/snowflake_sample_data/tpch_sf1/region", name = "region")]}
    )
    

create >> get_count >> query
