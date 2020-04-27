#### Installation:

pip3 install --ignore-installed git+ssh://git@github.com/atlanhq/atlan-airflow-lineage-plugin

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
