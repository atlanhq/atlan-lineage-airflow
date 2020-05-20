# Copyright 2020 Peeply Technologies Private Limited
#
# Licensed under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in
# compliance with the License. You may obtain a copy
# of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

from atlan.lineage.assets import SnowflakeTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Atlan", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="sample_dag", default_args=args, schedule_interval=None
)

table_name = "sample_table"

create_table_queries = [
    """DROP TABLE IF EXISTS "private.{}";""".format(table_name),
    """create table private.{} (name string, age number);""".format(table_name),
    """insert into private.{} values('Adam', 12),('Eve', 12),('Fate', 83);""".format(table_name),
]


def get_row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_common")
    result = dwh_hook.get_first("select count(*) from private.{}".format(table_name))
    logging.info("Number of rows in `private.%s`  - %s", table_name, result[0])


with dag:
    create_table = SnowflakeOperator(
        task_id="create_table",
        sql=create_table_queries,
        snowflake_conn_id="snowflake_common",
        outlets={"datasets": [SnowflakeTable(table_alias="cy25812.ap-southeast-1/demo_db/private/{}".format(table_name), name=table_name)]}
    )

    count_rows = PythonOperator(
        task_id="count_rows",
        python_callable=get_row_count,
        inlets={"datasets": [SnowflakeTable(table_alias="cy25812.ap-southeast-1/demo_db/private/{}".format(table_name), name=table_name)]}
    )

    copy_table = SnowflakeOperator(
        task_id="copy_table",
        sql="create table private.{}_copy as select * from private.{}".format(table_name, table_name),
        snowflake_conn_id="snowflake_common",
        inlets={"task_ids": ["create_table"]},
        outlets={"datasets": [SnowflakeTable(table_alias="cy25812.ap-southeast-1/demo_db/private/{}_copy".format(table_name), name="{}_copy".format(table_name))]}
    )

create_table >> [count_rows, copy_table]