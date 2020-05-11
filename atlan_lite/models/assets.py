import six

from typing import List
from jinja2 import Environment

from airflow.lineage.datasets import *
# from airflow.lineage.datasets import _inherited

import hashlib

class Entity(object):
    attributes = []  # type: List[str]
    type_name = ""

    def __init__(self, qualified_name=None, data=None, **kwargs):
        self._qualified_name = qualified_name
        self.context = None
        self._data = dict()

        self._data.update(dict((key, value) for key, value in six.iteritems(kwargs)
                               if key in set(self.attributes)))

        if data:
            if "qualifiedName" in data:
                self._qualified_name = data.pop("qualifiedName")

            self._data = dict((key, value) for key, value in six.iteritems(data)
                              if key in set(self.attributes))

    def set_context(self, context):
        self.context = context

    @property
    def qualified_name(self):
        if self.context:
            env = Environment()
            return env.from_string(self._qualified_name).render(**self.context)

        return self._qualified_name

    def get_guid(self):
        return int(str(int(hashlib.md5(self.name.encode()).hexdigest(), 16) * -1)[:10])


    def as_dict(self):
        attributes = dict(self._data)
        attributes.update({"qualifiedName": self.qualified_name})

        env = Environment()
        if self.context:
            for key, value in six.iteritems(attributes):
                attributes[key] = env.from_string(value).render(**self.context)

        d = {
            "typeName": self.type_name,
            "attributes": attributes,
            "guid": self.guid
        }
        return d


class Cluster(Entity):
    attributes = ["name"] # type: List[str]
    type_name = "cluster"
    def __init__(self, name=None, data=None):
        super(Entity, self).__init__(name=name, data=data)


class DataBase(Entity):
    type_name = "database"
    attributes = ["name"]
    def __init__(self, name=None, data=None):
        super(Entity, self).__init__(name=name, data=data)


class Schema(Entity):
    type_name = "schema"
    attributes = ["name"]
    def __init__(self, name=None, data=None):
        super(Entity, self).__init__(name=name, data=data)


class Table(DataSet):
    type_name = "table"
    attributes = ["name"]
    def __init__(self, name=None, data=None):
        super(DataSet, self).__init__(name=name, data=data)

class SnowflakeAccount(Cluster):
    type_name = "snowflake_cluster"
    attributes = ["name"]

    def __init__(self, name, data=None):
        super(Cluster, self).__init__(name=name, data=data)
        self.name = name
        self._qualified_name = 'cluster://' + self.name
        self.guid = self.get_guid()

class SnowflakeDatabase(DataBase):
    type_name = "snowflake_database"
    attributes = ["name"]

    def __init__(self, name, parent, data=None):
        super(DataBase, self).__init__(name=name, data=data)
        self.name = name
        self._qualified_name = parent['attributes']['name'] + '://' + self.name
        self.guid = self.get_guid()
        self._data['cluster'] = {
            'typeName': parent['typeName'],
            'guid': parent['guid']
        }

class SnowflakeSchema(Schema):
    type_name = "snowflake_schema"
    attributes = ["name"]
    
    def __init__(self, name, parent, data=None):
        super(Schema, self).__init__(name=name, data=data)
        self.name = name
        self._qualified_name = parent['attributes']['qualifiedName'] + '.' + self.name
        self.guid = self.get_guid()
        self._data['database'] = {
            'typeName': parent['typeName'],
            'guid': parent['guid']
        }


class SnowflakeTable(Table):
    type_name = "snowflake_table"
    attributes = ["name"]

    def __init__(self, name=None, table_alias = None, connection_id = None, data=None):
        super(Table, self).__init__(name=name, data=data)
        if name:
            parent = self.create_parent_entities(table_alias, connection_id)
            self.name = name
            self._qualified_name = parent['attributes']['qualifiedName'] + '/' + self.name
            # self.guid = self.get_guid()
            self._data['parentSchema'] = {
                'typeName': parent['typeName'],
                'guid': parent['guid']
            }

    def parse_alias(self, string):
        # snowflake_details = conn_string.split("@")[1]
        # warehouse = snowflake_details.split("?")[1].split("&")[0].split("=")[1]
        # snowflake_details = snowflake_details.split("?")[0]
        # snowflake_details = snowflake_details.split("/")
        # account = snowflake_details[0]
        # db = snowflake_details[1]
        # schema = snowflake_details[2]

        ## cy25812.ap-southeast-1/demo_db/public/tablename
        account = string.split('/')[0]
        db = string.split('/')[1]
        schema = string.split('/')[2]
        return account, db, schema

    def create_parent_entities(self, table_alias, connection_id):
        account, db, schema = self.parse_alias(table_alias)
        self.account = SnowflakeAccount(account)
        self.db = SnowflakeDatabase(db, self.account.as_dict())
        self.schema = SnowflakeSchema(schema, self.db.as_dict())
        return self.schema.as_dict()

    def as_nested_dict(self):
        d = self.as_dict()

        entities = []
        entities.append(self.account.as_dict())
        entities.append(self.db.as_dict())
        entities.append(self.schema.as_dict())
        entities.append(d)

        return entities

    def as_dict(self):
        attributes = dict(self._data)
        attributes.update({"qualifiedName": self.qualified_name})

        env = Environment()
        if self.context:
            for key, value in six.iteritems(attributes):
                try:
                    attributes[key] = env.from_string(value).render(**self.context)
                except Exception as e:
                    pass

        d = {
            "typeName": self.type_name,
            "attributes": attributes
        }
        return d

            
        

