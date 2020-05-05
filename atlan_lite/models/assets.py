import six

from typing import List
from jinja2 import Environment

from airflow.lineage.datasets import *
# TODO: fix the imports
from airflow.lineage.datasets import _inherited

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
        # if self.context:
        #     env = Environment()
        #     return env.from_string(self._qualified_name).render(**self.context)

        return self._qualified_name

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


class Server(Entity):
    attributes = ["name"] # type: List[str]
    type_name = "server"
    def __init__(self, name=None, data=None):
        super(Entity, self).__init__(name=name, data=data)


class Warehouse(Entity):
    type_name = "warehouse"
    attributes = ["name"]
    def __init__(self, name=None, data=None):
        super(Entity, self).__init__(name=name, data=data)


class DataBase(Entity):
    type_name = "database"
    attributes = ["name"]
    def __init__(self, name=None, data=None):
        super(Entity, self).__init__(name=name, data=data)


class Schema(Entity):
    type_name = "database_schema"
    attributes = ["name"]
    def __init__(self, name=None, data=None):
        super(Entity, self).__init__(name=name, data=data)


class Table(DataSet):
    type_name = "table"
    attributes = ["name"]
    def __init__(self, name=None, data=None):
        super(D, self).__init__(name=name, data=data)

class SnowflakeAccount(Server):
    type_name = "snowflake_account"
    attributes = ["name"]

    def __init__(self, name, data=None):
        super(Server, self).__init__(name=name, data=data)
        self._qualified_name = 'server://' + name
        self.guid = int(str(int(hashlib.md5(name.encode()).hexdigest(), 16) * -1)[:10])

class SnowflakeWarehouse(Warehouse):
    type_name = "snowflake_warehouse"
    attributes = ["name"]

    def __init__(self, name, superx, data=None):
        super(Warehouse, self).__init__(name=name, data=data)
        self._qualified_name = superx['attributes']['qualifiedName'] + '/' + name
        self.guid = int(str(int(hashlib.md5(name.encode()).hexdigest(), 16) * -1)[:10])
        self._data['server'] = {
            'typeName': superx['typeName'],
            'guid': superx['guid']
        }

class SnowflakeDatabase(DataBase):
    type_name = "snowflake_database"
    attributes = ["name"]

    def __init__(self, name, superx, data=None):
        super(DataBase, self).__init__(name=name, data=data)
        self._qualified_name = superx['attributes']['qualifiedName'] + '/' + name
        self.guid = int(str(int(hashlib.md5(name.encode()).hexdigest(), 16) * -1)[:10])
        self._data['warehouse'] = {
            'typeName': superx['typeName'],
            'guid': superx['guid']
        }

class SnowflakeSchema(Schema):
    type_name = "snowflake_schema"
    attributes = ["name"]
    
    def __init__(self, name, superx, data=None):
        super(Schema, self).__init__(name=name, data=data)
        self._qualified_name = superx['attributes']['qualifiedName'] + '/' + name
        self.guid = int(str(int(hashlib.md5(name.encode()).hexdigest(), 16) * -1)[:10])
        self._data['database'] = {
            'typeName': superx['typeName'],
            'guid': superx['guid']
        }


class SnowflakeTable(Table):
    type_name = "snowflake_table"
    attributes = ["name"]

    def __init__(self, name, string, data=None):
        # self.account, self.wh, self.db, self.schema, self.table = self.create_related_entities(string, table)
        print("TABLE: ", name)
        super(Table, self).__init__(name=name, data=data)
        superx = self.create_related_entities(string)
        print("SUPER:", superx)
        self._qualified_name = superx['attributes']['qualifiedName'] + '/' + name
        print("QUALIFIED NAME:", self._qualified_name)
        self.guid = int(str(int(hashlib.md5(name.encode()).hexdigest(), 16) * -1)[:10])
        self._data['database_schema'] = {
            'typeName': superx['typeName'],
            'guid': superx['guid']
        }

    def parse_conn_string(self, conn_string):
        snowflake_details = conn_string.split("@")[1]
        warehouse = snowflake_details.split("?")[1].split("&")[0].split("=")[1]
        snowflake_details = snowflake_details.split("?")[0]
        snowflake_details = snowflake_details.split("/")
        account = snowflake_details[0]
        db = snowflake_details[1]
        schema = snowflake_details[2]
        print("ACCOUNT: {}, WH: {}, DB: {}, SCHEMA: {}".format(account, warehouse, db, schema))
        return account, warehouse, db, schema

    def create_related_entities(self, string):
        account, wh, db, schema = self.parse_conn_string(string)
        self.account = SnowflakeAccount(account)
        self.wh = SnowflakeWarehouse(wh, self.account.as_dict())
        self.db = SnowflakeDatabase(db, self.wh.as_dict())
        self.schema = SnowflakeSchema(schema, self.db.as_dict())
        return self.schema.as_dict()

    def as_dict(self):
        attributes = dict(self._data)
        attributes.update({"qualifiedName": self.qualified_name})

        env = Environment()
        # if self.context:
        #     for key, value in six.iteritems(attributes):
        #         attributes[key] = env.from_string(value).render(**self.context)
        print("OVER RIDING METHOD")
        d = {
            "typeName": self.type_name,
            "attributes": attributes,
        }


        entities = []
        entities.append(self.account.as_dict())
        entities.append(self.wh.as_dict())
        entities.append(self.db.as_dict())
        entities.append(self.schema.as_dict())
        entities.append(d)

        return entities

            
        

