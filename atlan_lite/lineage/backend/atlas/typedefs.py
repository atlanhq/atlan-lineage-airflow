from airflow.lineage.backend.atlas.typedefs import operator_typedef

## TODO: sep. these entity defs out into sep. variables

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
        'serviceType': 'atlan',
        'endDef1': {
          'type': 'airflow_dag',
          'name': 'tasks',
          'isContainer': True,
          'cardinality': 'SET',
          'isLegacyAttribute': True
        },
        'endDef2': {
          'type': 'airflow_operator',
          'name': 'dag',
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
        'name': 'database',
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
        'name': 'schema',
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
        'name': 'table',
        'isContainer': True,
        'cardinality': 'SET',
        'isLegacyAttribute': True
      },
      'propagateTags': 'NONE'
    }
  ]

}