
### Plugin to send airflow lineage data to Atlan

#### Installation:

`pip3 install --ignore-installed git+ssh://git@github.com/atlanhq/atlan-airflow-lineage-plugin`

#### Add the following in airflow.cfg
1. Search for `[lineage]` and add the following
```
[lineage]
# what lineage backend to use
backend = atlan_lite.lineage.backend.atlan.AtlanBackend
```

2. Add the following: 
```
[atlas]
sasl_enabled = False
host = host
port = port
username = username
password = password
```

Check Airflow Lineage docs [here](https://airflow.apache.org/docs/stable/lineage.html)

## Sample DAG

```
import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

from atlan_lite.models.assets import SnowflakeTable, File


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="atlan_lite_lineage", default_args=args, schedule_interval=None
)


def fetch_data(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_pandas_df("select * from TPCH_SF1.NATION")
    print(result)


def row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("select count(*) from TPCH_SF1.NATION")
    logging.info("Number of rows in `public.test_table`  - %s", result[0])

with dag:
    create_insert = PythonOperator(
        task_id="create_insert", 
        python_callable=fetch_data,
        inlets = {'datasets':[SnowflakeTable(conn_string="snowflake://username:pass@accountname/databasename/schema?warehouse=warehouse", name = "table_name")]},
        outlets={"datasets": [File("/tmp/final/output.csv")]}
    )

    get_count = PythonOperator(
        task_id="get_count", 
        python_callable=row_count,
        inlets={"auto": True},
        outlets={"datasets":[File("/tmp/final/output2.csv")]}
    )

    query = SnowflakeOperator(
        task_id="query_snowfalke",
        sql="select count(*) from TPCH_SF10.REGION",
        snowflake_conn_id="snowflake_conn",
        inlets={"task_ids": ["create_insert", "get_count"]},
        outlets={"datasets":[SnowflakeTable(conn_string="snowflake://username:pass@accountname/databasename/schema?warehouse=warehouse", name = "table_name")]}
    )

create_insert >> get_count  >> query
```

