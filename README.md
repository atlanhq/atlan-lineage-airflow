## Atlan Airflow Lineage Plugin
#### This plugin allows you to send lineage metadata from Airflow to Atlan

Data lineage helps you keep track of the origin of data, the transformations done on it over time  and its impact in an organization. Airflow has [built-in support](https://airflow.apache.org/docs/stable/lineage.html) to send lineage metadata to Apache Atlas. This plugin leverages that and enables you to create lineage metadata for operation on Snowflake entities. This lineage can then be viewed on Atlas or [Atlan](https://atlan.com)


### Prerequisites
You need to have the following setup before you can start using this:
1. [Apache Airflow](https://airflow.apache.org/docs/stable/start.html)
2. [Apache Atlas](http://atlas.apache.org)
3. [Snowflake Account](https://www.snowflake.com)


### Installation

`pip install atlan-airflow-lineage-plugin`

#### Enable plugin
1. To send lineage to Atlas, follow the instructions given [here](https://airflow.apache.org/docs/stable/lineage.html#apache-atlas). Change `backend` to `atlan.lineage.backend.Atlas`
```
[lineage]
backend = atlan.lineage.backend.Atlas
```

2. To send lineage to Atlan, change the `backend` value in airflow.cfg like so:
```
[lineage]
backend = atlan.lineage.backend.Atlan
```
Generate an access token on Atlan and add the following to airflow.cfg
```
[atlan]
url = domain.atlan.com
token = 'my-secret-token' 
```
The value of `url` should be the URL of your Atlan instance.

### Tutorial

1. At the top of dag file, import the modules for entity you want create lineage for

```
from atlan.lineage.assets import SnowflakeTable
```


2. For every task, configure Airflow operator’s inlets and outlets parameters

```python
# Sample task
# Creates Snowflake table `my_new_table` from Snowflake table `my_table`
# Inlet for task - Snowflake table 'my_table'
# Outlet for task - file 'my_new_table'
# Let snowflake account name: mi04151.ap-south-1
# Let snowflake database name: biw 
# Let snowflake schema name: public

sample_task = SnowflakeOperator(
    task_id = "sample_task",
    
    ## query snowflake
    sql = "CREATE TABLE MY_NEW_TABLE AS SELECT * FROM MY_TABLE",
    
    ## snowflake connection id as configured in Airflow connections                                  
    snowflake_conn_id = "snowflake_common",
    
    ## define inlets
    inlets: {                                                      
      "datasets": [SnowflakeTable(table_alias = "mi04151.ap-south-1/biw/public/my_table",    
                                  name = "my_table")]    
    },
    
    ## define outlets
    outlets: {                                                    
      "datasets": [SnowflakeTable(table_alias = "mi04151.ap-south-1/biw/public/my_new_table",    
                                  name = "my_new_table")]
    }
  )

```

#### Instantiating a SnowflakeTable
SnowflakeTable takes 2 arguments - `table_alias` and `name`:
1. `table_alias`: This is a string representing the location of a Snowflake table i.e. the account, database, schema a table is present in. It follows format `snowflake-account/snowflake-database/snowflake-schema/snowflake-table`, where `snowflake-account` is the name of your snowflake account, etc.
Read more about Snowflake account naming (here)[https://docs.snowflake.com/en/user-guide/python-connector-api.html#usage-notes-for-the-account-parameter-for-the-connect-method].
2. `name`: This is the name of the table in question

Let's call this `lineage-object`

#### Anatomy of Airflow Inlets and Outlets
`Inlets` and `Outlets` are parameters of Airflow operators that allow us to specify task input and output. They follow the following format:
```python
{
    "key": [
        lineage-object_1,
        lineage-object_2,
        .
        .
        .
    ]

}
```
Only keys `datasets`, `task_ids`, `auto` are accepted. If the key is `auto`, then value should be `True` and not a list

This plugin supports the [Airflow API](https://airflow.apache.org/docs/stable/lineage.html) to create inlets and outlets. So inlets can be defined in the following ways:
* by a list of dataset {"datasets": [dataset1, dataset2]}
* can be configured to look for outlets from upstream tasks {"task_ids": ["task_id1", "task_id2"]}
* can be configured to pick up outlets from direct upstream tasks {"auto": True}
* a combination of them



#### YAML DAG

```YAML
## task definition
customer_nation_join:
    operator: airflow.contrib.operators.snowflake_operator.SnowflakeOperator
    sql: CREATE TABLE MY_NEW_TABLE AS SELECT * FROM MY_TABLE
    snowflake_conn_id: snowflake_common
    inlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/public/my_table", name = "my_table")]}'
    outlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/public/my_new_table", name = "my_new_table")]}'

```
The inlets and outlets are defined same as above, just the dictionary is enclosed in quotes.


##### Note:- We used [dag-factory](https://github.com/ajbosco/dag-factory) to create sample YAML dags. We made some changes to enable support for `inlets` & `outlets` parameters.

### Example DAG

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
```


In the above dag, we have three tasks:

1. `customer_nation_join` : This task joins two Snowflake tables `nation` and `customer` and creates the Snowflake table `customer_enriched`. The input of of this task are Snowflake tables `nation` and `customer`, and the output `customer_enriched` table, so this is what task inlets and outlets are configured as.
2. `filter_apac`: This task filters out the customers in table `customer_enriched` that lies in APAC nations and creates the table `customer_apac`. The input of this task is the output of the upstream task and the output is `customer_apac` 
3. `aggregate_apac`: This task counts the customers present in each APAC nation and creates table `customer_distribition`. The input of this table is the output of upstream task and output is table `customer_distribution`




![Airflow DAG](/images/airflow_dag_readme_example.png)
This is what the DAG looks like on Airflow



![Lineage on Atlas](/images/atlas_lineage_readme_example.png)
This is what lineage from the dag above is represented in Atlas - you can clearly see which table was produced from which task, which task has which table as input, etc.

The icons in green represent Airflow task - one can see the inputs and outputs for each task. 
The yellow arrows represent lineage of an entity and the red arrows represent impact of the entity. 


![DAG Entity on Atlas](/images/atlas_dag_entity_readme_example.png)
This is what the Airflow DAG entity looks on Atlas. You can see the tasks present in the dag, along with other meta.


![DAG Entity on Atlas](/images/atlas_op_entity_readme_example.png)
This is what the Airflow Operator entity looks on Atlas. You can see the DAG that the operator is part of, the inputs and outputs for the operator.

#### Sample YAML DAG

If you are using YAML configs to create Airflow DAGs, this is what the above dag would look like

```YAML
customer_distribution_apac:
  default_args:
    owner: 'Atlan'
    start_date: 2020-01-01 
    retries: 1
    retry_delay_sec: 30
  description: Create an aggregated table to see distribution of customers across APAC nations
  schedule_interval: '0 0 * 12 0'
  concurrency: 1
  max_active_runs: 1
  dagrun_timeout_sec: 60
  default_view: 'tree' 
  orientation: 'LR'
  tasks:
    ## task definition
    customer_nation_join:
      operator: airflow.contrib.operators.snowflake_operator.SnowflakeOperator
      sql: create table biw.private.customer_enriched as select c.c_custkey, c.c_acctbal, c.c_mktsegment, n.n_nationkey, n.n_name from biw.raw.customer c inner join biw.raw.nation n on c.c_nationkey = n.n_nationkey
      snowflake_conn_id: "snowflake_common"
      inlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/raw/customer", name = "customer"),SnowflakeTable(table_alias="mi04151.ap-south-1/biw/raw/nation", name = "nation")]}'
      outlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/private/customer_enriched", name = "customer_enriched")]}'
    ## task definition
    filter_apac:
      operator: airflow.contrib.operators.snowflake_operator.SnowflakeOperator
      sql: create table biw.private.customer_apac as select * from biw.private.customer_enriched where n_name in ('CHINA', 'INDIA', 'INDONESIA', 'VIETNAM', 'PAKISTAN', 'NEW ZEALEAND', 'AUSTRALIA')
      snowflake_conn_id: "snowflake_common"
      inlets: '{"auto":True}'
      outlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/private/customer_apac", name = "customer_apac")]}'
      dependencies: [customer_nation_join]
    ## task definition
    aggregate_apac:
      operator: airflow.contrib.operators.snowflake_operator.SnowflakeOperator
      sql: create table biw.cubes.customer_distribution as select count(*) as num_customers, n_name as nation from biw.private.customer_apac group by n_name
      snowflake_conn_id: "snowflake_common"
      inlets: '{"auto":True}'
      outlets: '{"datasets":[SnowflakeTable(table_alias="mi04151.ap-south-1/biw/cubes/customer_distribution", name = "customer_distribution")]}'
      dependencies: [filter_apac]

```

##### Note:- We used [dag-factory](https://github.com/ajbosco/dag-factory) to create sample YAML dags. We made some changes to enable support for `inlets` & `outlets` parameters.

##### _Sample dags can be found in **examples** folder_



