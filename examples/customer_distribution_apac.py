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

## DAG name: customer_distribution_apac

import logging

import airflow
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

from atlan.lineage.assets import SnowflakeTable

args = {"owner": "Atlan", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(dag_id="customer_distribution_apac", default_args=args, schedule_interval=None)

with dag:
    ## task definition
  customer_nation_join = SnowflakeOperator(
    task_id = "customer_nation_join",
    sql = """CREATE TABLE biw.private.customer_enriched AS
             SELECT c.c_custkey,
                c.c_acctbal,
                c.c_mktsegment,
                n.n_nationkey,
                n.n_name
             FROM biw.raw.customer c
             INNER JOIN biw.raw.nation n ON c.c_nationkey = n.n_nationkey""",
    snowflake_conn_id = "snowflake_common",
    inlets: {
      "datasets": [SnowflakeTable(table_alias = "mi04151.ap-south-1/biw/raw/customer", 
                                  name = "customer"), 
                   SnowflakeTable(table_alias = "mi04151.ap-south-1/biw/raw/nation", 
                                  name = "nation")]
    },
    outlets: {
      "datasets": [SnowflakeTable(table_alias = "mi04151.ap-south-1/biw/private/customer_enriched", 
                                  name = "customer_enriched")]
    }
  )

    ## task definition
    filter_apac = SnowflakeOperator(
    task_id = "filter_apac",
    sql = """CREATE TABLE biw.private.customer_apac AS
            SELECT *
            FROM biw.private.customer_enriched
            WHERE n_name IN ('CHINA',
                            'INDIA',
                            'INDONESIA',
                            'VIETNAM',
                            'PAKISTAN',
                            'NEW ZEALAND',
                            'AUSTRALIA')""",
    snowflake_conn_id = "snowflake_common",
    inlets: {
        "auto": True
    },
    outlets: {
        "datasets": [SnowflakeTable(table_alias = "mi04151.ap-south-1/biw/private/customer_apac", 
                                    name = "customer_apac")]
    }
    )
        ## task definition
    aggregate_apac = SnowflakeOperator(
    task_id = "aggregate_apac",
    sql = """CREATE TABLE biw.cubes.customer_distribution AS
            SELECT count(*) AS num_customers,
                n_name AS nation
            FROM biw.private.customer_apac
            GROUP BY n_name""",
    snowflake_conn_id = "snowflake_common",
    inlets: {
        "auto": True
    }
    outlets: {
        "datasets": [SnowflakeTable(table_alias = "mi04151.ap-south-1/biw/cubes/customer_distribution", 
                                    name = "customer_distribution")]
    }
    )

customer_nation_join >> filter_apac >> aggregate_apac