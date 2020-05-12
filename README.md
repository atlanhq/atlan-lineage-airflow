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
[atlan]
sasl_enabled = False
host = host
port = port
username = username
password = password
```

#### Caveats
1. Dependent on airflow's lineage modules
2. The corresponding entity and relationship definitions for Atlas reside in Caspian
=======

### Plugin to send airflow lineage data to Atlan

This plugin extends Airflow's native lineage support to include Snowflake entities. 

Check Airflow Lineage docs [here](https://airflow.apache.org/docs/stable/lineage.html)

#### Prerequisites
1. Atlas setup
2. Airflow setup
3. Snowflake setup

#### Installation:

`pip3 install --ignore-installed git+ssh://git@github.com/atlanhq/atlan-airflow-lineage-plugin`

#### Add the following in airflow.cfg
1. Search for `[lineage]` and add the following
```
[lineage]
# what lineage backend to use
backend = atlan_lite.lineage.backend.atlan.AtlasBackend
```

2. Add the following: 
```
[atlas]
sasl_enabled = False
host = atlas_host
port = atlas_port
username = atlas_username
password = atlan_password
```

#### How to use

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

