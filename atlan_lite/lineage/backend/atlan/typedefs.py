from airflow.lineage.backend.atlas.typedefs import operator_typedef

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
            'name': 'snowflake_account',
            'typeVersion': '2.0',
            'serviceType': 'atlan',
            'description': 'Snowflake Account'
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
        'name': 'schema',
        'isContainer': False,
        'cardinality': 'SINGLE',
        'isLegacyAttribute': True
      },
      'endDef2': {
        'type': 'parentSchema',
        'name': 'table',
        'isContainer': True,
        'cardinality': 'SET',
        'isLegacyAttribute': True
      },
      'propagateTags': 'NONE'
    }
  ]

}