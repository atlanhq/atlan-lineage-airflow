from airflow.lineage.backend.atlas.typedefs import operator_typedef

entity_typedef = {
    'entityDefs': [
        {
            'superTypes': [
                'Asset'
            ],
            'name': 'server',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Server Entity'
        },
        {
            'superTypes': [
                'Asset'
            ],
            'name': 'warehouse',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Warehouse Entity'
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
            'name': 'database_schema',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Database Schema Entity'
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
                'server'
            ],
            'name': 'snowflake_account',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Snowflake Account'
        },
        {
            'superTypes': [
                'warehouse'
            ],
            'name': 'snowflake_warehouse',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Snowflake Warehouse'
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
                'database_schema'
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
        }
    ],
  'relationshipDefs': [
      {
      'name': 'belongs_to_server',
      'typeVersion': '2.0',
      'relationshipCategory': 'AGGREGATION',
      'serviceType': 'atlan',
      'endDef1': {
        'type': 'warehouse',
        'name': 'server',
        'isContainer': False,
        'cardinality': 'SINGLE',
        'isLegacyAttribute': True
      },
      'endDef2': {
        'type': 'server',
        'name': 'warehouse',
        'isContainer': True,
        'cardinality': 'SET',
        'isLegacyAttribute': True
      },
      'propagateTags': 'NONE'
    },
    {
      'name': 'belongs_to_warehouse',
      'typeVersion': '2.0',
      'relationshipCategory': 'AGGREGATION',
      'serviceType': 'atlan',
      'endDef1': {
        'type': 'database',
        'name': 'warehouse',
        'isContainer': False,
        'cardinality': 'SINGLE',
        'isLegacyAttribute': True
      },
      'endDef2': {
        'type': 'warehouse',
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
        'type': 'database_schema',
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
        'name': 'schema',
        'isContainer': False,
        'cardinality': 'SINGLE',
        'isLegacyAttribute': True
      },
      'endDef2': {
        'type': 'database_schema',
        'name': 'table',
        'isContainer': True,
        'cardinality': 'SET',
        'isLegacyAttribute': True
      },
      'propagateTags': 'NONE'
    }
  ]

}