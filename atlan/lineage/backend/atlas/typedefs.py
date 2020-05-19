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

from airflow.lineage.backend.atlas.typedefs import operator_typedef  # type: ignore # noqa: F401, F403, E501


# TODO: sep. these entity defs out into sep. variables

entity_typedef = {
    'entityDefs': [
        {
            'superTypes': [
                'Asset'
            ],
            'name': 'cluster',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Cluster Entity'
        },
        {
            'superTypes': [
                'Asset'
            ],
            'name': 'database',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Database Entity'
        },
        {
            'superTypes': [
                'Asset'
            ],
            'name': 'schema',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Schema Entity'
        },
        {
            'superTypes': [
                'DataSet'
            ],
            'name': 'table',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Table Entity'
        },
        {
            'superTypes': [
                'cluster'
            ],
            'name': 'snowflake_cluster',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Snowflake Cluster'
        },
        {
            'superTypes': [
                'database'
            ],
            'name': 'snowflake_database',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Snowflake Database'
        },
        {
            'superTypes': [
                'schema'
            ],
            'name': 'snowflake_schema',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Snowflake Schema'
        },
        {
            'superTypes': [
                'table'
            ],
            'name': 'snowflake_table',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Snowflake Table'
        },
        {
          'superTypes': [
                'Asset'
            ],
          'name': 'airflow_dag',
          'typeVersion': '2.0',
          'serviceType': 'atlan',
          'description': 'Airflow DAG',
          'attributeDefs': [
              {
                'name': 'dag_id',
                'typeName': 'string',
                'isOptional': False,
                'cardinality': "SINGLE",
                'isUnique': False,
                'isIndexable': True
              },
              {
                'name': 'execution_date',
                'typeName': 'date',
                'isOptional': False,
                'cardinality': "SINGLE",
                'isUnique': False,
                'isIndexable': True
              },
              {
                'name': 'run_id',
                'typeName': 'string',
                'isOptional': False,
                'cardinality': "SINGLE",
                'isUnique': False,
                'isIndexable': True
              }
            ]
        }
    ],
    'relationshipDefs': [
      {
        'name': 'has_task',
        'typeVersion': '2.0',
        'relationshipCategory': 'COMPOSITION',
        'relationshipLabel': '__airflow_dag.airflow_operators',
        'serviceType': 'atlan',
        'endDef1': {
          'type': 'airflow_dag',
          'name': 'airflow_operators',
          'isContainer': True,
          'cardinality': 'SET',
          'isLegacyAttribute': True
        },
        'endDef2': {
          'type': 'airflow_operator',
          'name': 'airflow_dag',
          'isContainer': False,
          'cardinality': 'SINGLE',
          'isLegacyAttribute': True
        },
        'propagateTags': 'NONE'
      },
      {
        'name': 'belongs_to_cluster',
        'typeVersion': '2.0',
        'relationshipCategory': 'AGGREGATION',
        'relationshipLabel': '__database.cluster',
        'serviceType': 'atlan',
        'endDef1': {
          'type': 'database',
          'name': 'cluster',
          'isContainer': False,
          'cardinality': 'SINGLE',
          'isLegacyAttribute': True
        },
        'endDef2': {
          'type': 'cluster',
          'name': 'databases',
          'isContainer': True,
          'cardinality': 'SET',
          'isLegacyAttribute': True
        },
        'propagateTags': 'NONE'
      },
      {
        'name': 'belongs_to_database',
        'typeVersion': '2.0',
        'relationshipCategory': 'AGGREGATION',
        'relationshipLabel': '__schema.database',
        'serviceType': 'atlan',
        'endDef1': {
          'type': 'schema',
          'name': 'database',
          'isContainer': False,
          'cardinality': 'SINGLE',
          'isLegacyAttribute': True
        },
        'endDef2': {
          'type': 'database',
          'name': 'schemas',
          'isContainer': True,
          'cardinality': 'SET',
          'isLegacyAttribute': True
        },
        'propagateTags': 'NONE'
      },
      {
        'name': 'belongs_to_schema',
        'typeVersion': '2.0',
        'relationshipCategory': 'AGGREGATION',
        'relationshipLabel': '__table.schema',
        'serviceType': 'atlan',
        'endDef1': {
          'type': 'table',
          'name': 'parentSchema',
          'isContainer': False,
          'cardinality': 'SINGLE',
          'isLegacyAttribute': True
        },
        'endDef2': {
          'type': 'schema',
          'name': 'tables',
          'isContainer': True,
          'cardinality': 'SET',
          'isLegacyAttribute': True
        },
        'propagateTags': 'NONE'
      }
    ]
}
