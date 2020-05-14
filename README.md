### Plugin to send airflow lineage data to Atlan

Data lineage helps you keep track of the origin of data, the transformations done on it over time  and its impact in an organization. Airflow has [built-in support](https://airflow.apache.org/docs/stable/lineage.html) to send lineage metadata to Apache Atlas. This plugin leverages that and enables you to create lineage metadata for Snowflake operations.


Let's take a look at an example dag and see what the result looks like on Atlas:

```python
## DAG name: example_dag3

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
    dag_id="example_dag3", default_args=args, schedule_interval=None
)

table_name = "sample_table"

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
        inlets={"auto": True}, 
        outlets={"datasets":[f]}
    )
    

create >> get_count
```



In the above dag, we have two tasks:

1. `create` : This task creates a table on Snowflake and populates it with some data. The output of this task is the Snowflake table `sample_table`, so this is what we configure for  the task `outlets` parameter. 
2. `get_count`: This task gets the number of rows present in the table created in `create` task. The input of this task is the output of the upstream task, and lets assume the output is a file. 

###### NOTE: This plugin supports Airflow convention for defining inlets and outlets

This is what lineage from the dag above is represented in Atlas:

![Lineage on Atlas](/images/atlas_lineage_readme_example.png)


The icons in green represent Airflow operators - one can see the inputs and outputs for each operator. 


#### Installation:

`pip3 install --ignore-installed git+ssh://git@github.com/atlanhq/atlan-airflow-lineage-plugin`

Follow the instructions given [here](https://airflow.apache.org/docs/stable/lineage.html#apache-atlas)

Just change `backend` to `atlan_lite.lineage.backend.atlan.AtlasBackend`

#### Usage

1. Package import 

```
from atlan_lite.models.assets import SnowflakeTable
```

2. Specify Snowflake table in inlet/outlet

```
SnowflakeTable(table_alias = "snowflake-account-name/snowflake-database-name/snowflake-schema-name/snowflake-table-name",
                name = "snowflake-table-name")

```

*Sample dags can be found in the folder* ***sample_dags***


#### Prerequisites
You need to have the following setup before you can start using this:
1. [Apache Airflow](https://airflow.apache.org/docs/stable/start.html)
2. [Apache Atlas](http://atlas.apache.org)
3. [Snowflake](https://www.snowflake.com)
