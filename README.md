### Plugin to send airflow lineage data to Atlan

Data lineage helps you keep track of the origin of data, the transformations done on it over time  and its impact in an organization. Airflow has [built-in support](https://airflow.apache.org/docs/stable/lineage.html) to send lineage metadata to Apache Atlas. This plugin leverages that and enables you to create lineage metadata for operation on Snowflake entities.


Let's take a look at an example dag and see what the result looks like on Atlas:

```python
## DAG name: customer_distribution_apac

import logging

import airflow
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

from atlan.lineage.assets import SnowflakeTable

args = {"owner": "Atlan", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(dag_id="customer_distribution_apac", default_args=args, schedule_interval=None)

with dag:
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
                           'NEW ZEALEAND',
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
```


In the above dag, we have three tasks:

1. `customer_nation_join` : This task joins two Snowflake tables `nation` and `customer` and creates the Snowflake table `customer_enriched`. The input of of this task are Snowflake tables `nation` and `customer`, and the output `customer_enriched` table, so this is what task inlets and outlets are configured as.
2. `filter_apac`: This task filters out the customers in table `customer_enriched` that lies in APAC nations and creates the table `customer_apac`. The input of this task is the output of the upstream task and the output is `customer_apac` 
3. `aggregate_apac`: This task counts the customers present in each APAC nation and creates table `customer_distribition`. The input of this table is the output of upstream task and output is table `customer_distribution`

This is what lineage from the dag above is represented in Atlas:

![Lineage on Atlas](/images/atlas_lineage_readme_example.png)

The icons in green represent Airflow task - one can see the inputs and outputs for each task.

If you are using YAML configs to create Airflow DAGs, this is what the above dag would look like

```YAML
customer_distribution_apac:
  default_args:
    owner: 'Atlan'
    start_date: 2020-01-01  # or '2 days'
    retries: 1
    retry_delay_sec: 30
  description: Create an aggregated table to see distribution of customers across APAC nations
  schedule_interval: '0 0 * 12 0'
  concurrency: 1
  max_active_runs: 1
  dagrun_timeout_sec: 60
  default_view: 'tree'  # or 'graph', 'duration', 'gantt', 'landing_times'
  orientation: 'LR'  # or 'TB', 'RL', 'BT'
  tasks:
    customer_nation_join:
      operator: airflow.contrib.operators.snowflake_operator.SnowflakeOperator
      sql: create table biw.private.customer_enriched as select c.c_custkey, c.c_acctbal, c.c_mktsegment, n.n_nationkey, n.n_name from biw.raw.customer c inner join biw.raw.nation n on c.c_nationkey = n.n_nationkey
      snowflake_conn_id: "snowflake_common"
      inlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/raw/customer", name = "customer"),SnowflakeTable(table_alias="mi04151.ap-south-1/biw/raw/nation", name = "nation")]}'
      outlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/private/customer_enriched", name = "customer_enriched")]}'
    filter_apac:
      operator: airflow.contrib.operators.snowflake_operator.SnowflakeOperator
      sql: create table biw.private.customer_apac as select * from biw.private.customer_enriched where n_name in ('CHINA', 'INDIA', 'INDONESIA', 'VIETNAM', 'PAKISTAN', 'NEW ZEALEAND', 'AUSTRALIA')
      snowflake_conn_id: "snowflake_common"
      inlets: '{"auto":True}'
      outlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/private/customer_apac", name = "customer_apac")]}'
      dependencies: [customer_nation_join]
    aggregate_apac:
      operator: airflow.contrib.operators.snowflake_operator.SnowflakeOperator
      sql: create table biw.cubes.customer_distribution as select count(*) as num_customers, n_name as nation from biw.private.customer_apac group by n_name
      snowflake_conn_id: "snowflake_common"
      inlets: '{"auto":True}'
      outlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/cubes/customer_distribution", name = "customer_distribution")]}'
      dependencies: [filter_apac]

```

##### Note:- We used [dag-factory](https://github.com/ajbosco/dag-factory) to create sample YAML dags. We made some changes to enable support for `inlets` & `outlets` parameters. You can find the path at [](https://github.com/atlanhq/dag-factory)

##### _Sample dags can be found in **examples** folder_

This plugin supports the [Airflow API](https://airflow.apache.org/docs/stable/lineage.html) to create inlets and outlets. So inlets can be defined in the following ways:
* by a list of dataset {"datasets": [dataset1, dataset2]}
* can be configured to look for outlets from upstream tasks {"task_ids": ["task_id1", "task_id2"]}
* can be configured to pick up outlets from direct upstream tasks {"auto": True}
* a combination of them


#### Installation:

`pip3 install --ignore-installed git+ssh://git@github.com/atlanhq/atlan-airflow-lineage-plugin`


#### Enable plugin
1. To send lineage to Atlas, follow the instructions given [here](https://airflow.apache.org/docs/stable/lineage.html#apache-atlas). Just change `backend` to `atlan.lineage.backend.Atlas`

2. To send lineage to Atlan, change the `backend` value in airflow.cfg like so:
```
[lineage]
backend = atlan.lineage.backend.Atlan
```
Generate an access token on Atlan and add the following to airflow.cfg
```
[atlan]
url = lite.atlan.com/api/v1/caspian
token = 'my-secret-token' 
```

#### Usage

1. Package import: At the top of dag file, import the relevant entity

```
from atlan.lineage.assets import SnowflakeTable
```

2. Specify Snowflake table in inlet/outlet

```
SnowflakeTable(table_alias = "snowflake-account-name/snowflake-database-name/snowflake-schema-name/snowflake-table-name",
                name = "snowflake-table-name")
```


#### Prerequisites
You need to have the following setup before you can start using this:
1. [Apache Airflow](https://airflow.apache.org/docs/stable/start.html)
2. [Apache Atlas](http://atlas.apache.org)
3. [Snowflake](https://www.snowflake.com)
