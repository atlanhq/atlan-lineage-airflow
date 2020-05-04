import six

from typing import List
from jinja2 import Environment

from airflow.lineage.datasets import *
from airflow.lineage.datasets import _inherited

import hashlib

# def attach_guid(self):
#     self._data['guid'] = int(str(int(hashlib.md5(self.name.encode()).hexdigest(), 16) * -1)[:10])

class Infrastructure(object):
    attributes = []  # type: List[str]
    type_name = "infrastructure"


class Warehouse(DataSet):
    type_name = "warehouse"
    attributes = []


class Schema(DataSet):
    type_name = "schema"
    attributes = []


class Table(DataSet):
    type_name = "table"
    attributes = ["name"]

class SnowflakeAccount(Infrastructure):
    type_name = "snowflake_account"
    attributes = []

    def __init__(self, name, data=None):
        super(Infrastructure, self).__init__(name=name, data=data)
        self._qualified_name = 'cluster://' + name
        self._data['guid'] = int(str(int(hashlib.md5(self.name.encode()).hexdigest(), 16) * -1)[:10])

class SnowflakeWarehouse(Warehouse):
    type_name = "snowflake_warehouse"
    attributes = []

    def __init__(self, name, super, data=None):
        super(Warehouse, self).__init__(name=name, data=data)
        self._qualified_name = super['attributes']['qualifiedName'] + '/' + name
        self._data['guid'] = int(str(int(hashlib.md5(self.name.encode()).hexdigest(), 16) * -1)[:10])
        self._data['account'] = {
            'typeName': super['typeName'],
            'guid': super['attributes']['guid']
        }

class SnowflakeDatabase(DataBase):
    type_name = "snowflake_database"
    attributes = []

    def __init__(self, name, super, data=None):
        super(DataBase, self).__init__(name=name, data=data)
        self._qualified_name = super['attributes']['qualifiedName'] + '/' + name
        self._data['guid'] = int(str(int(hashlib.md5(self.name.encode()).hexdigest(), 16) * -1)[:10])
        self._data['warehouse'] = {
            'typeName': super['typeName'],
            'guid': super['attributes']['guid']
        }

class SnowflakeSchema(Schema):
    type_name = "snowflake_schema"
    attributes = []
    
    def __init__(self, name, super, data=None):
        super(Schema, self).__init__(name=name, data=data)
        self._qualified_name = super['attributes']['qualifiedName'] + '/' + name
        self._data['guid'] = int(str(int(hashlib.md5(self.name.encode()).hexdigest(), 16) * -1)[:10])
        self._data['database'] = {
            'typeName': super['typeName'],
            'guid': super['attributes']['guid']
        }


class SnowflakeTable(Table):
    type_name = "snowflake_table"
    attributes = []

    def __init__(self, name, string, super = None, data=None):
        self.account, self.wh, self.db, self.schema, self.table = self.create_related_entities(string, table)
        super(Table, self).__init__(name=name, data=data)
        super = self.create_related_entities(string)
        self._qualified_name = super['attributes']['qualifiedName'] + '/' + name
        self._data['guid'] = int(str(int(hashlib.md5(self.name.encode()).hexdigest(), 16) * -1)[:10])
        self._data['schema'] = {
            'typeName': super['typeName'],
            'guid': super['attributes']['guid']
        }

    def parse_conn_string(self, conn_string):
        snowflake_details = conn_string.split("@")[1]
        warehouse = snowflake_details.split("?")[1].split("&")[0].split("=")[1]
        snowflake_details = snowflake_details.split("?")[0]
        snowflake_details = snowflake_details.split("/")
        account = snowflake_details[0]
        db = snowflake_details[1]
        schema = snowflake_details[2]
        return account, warehouse, db, schema

    def create_related_entities(self, string):
        account, wh, db, schema = self.parse_conn_string(string, table)
        account = SnowflakeAccount(account)
        wh = SnowflakeWarehouse(wh, account.as_dict())
        db = SnowflakeDatabase(db, wh.as_dict())
        schema = SnowflakeSchema(schema, db.as_dict())
        return schema.as_dict()


    def as_dict(self):
        table_dict = super().as_dict()
        account_dict = self.account.as_dict()
        wh_dict = self.wh.as_dict()
        db_dict = self.db.as_dict()
        schema_dict = self.schema.as_dict()



            
        

