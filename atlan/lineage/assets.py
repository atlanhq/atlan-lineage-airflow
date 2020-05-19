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

import six

from typing import List, Tuple, Union
from jinja2 import Environment

from airflow.lineage.datasets import *  # type: ignore # noqa: F401, F403
from airflow.lineage.datasets import DataSet  # type: ignore

import hashlib


class Entity(object):
    attributes = []  # type: List[str]
    type_name = ""

    def __init__(self, qualified_name=None, data=None, **kwargs):
        self._qualified_name = qualified_name
        self.context = None
        self._guid = None
        self._data = dict()

        if "name" in kwargs:
            self._name = kwargs['name']

        self._data.update(dict((key, value) for key, value in six.iteritems(
                                                                        kwargs)
                               if key in set(self.attributes)))

        if data:
            if "qualifiedName" in data:
                self._qualified_name = data.pop("qualifiedName")

            if "name" in data:
                self._name = data["name"]

            if "guid" in data:
                self._guid = data.pop("guid")

            self._data = dict((key, value) for key, value in six.iteritems(
                                                                        data)
                              if key in set(self.attributes))

    def set_context(self, context):
        # type: (dict) -> None
        self.context = context

    @property
    def qualified_name(self):
        # type: () -> str
        if self.context:
            env = Environment()
            return env.from_string(self._qualified_name).render(**self.context)

        return self._qualified_name

    @property
    def name(self):
        # type: () -> str
        return self._name

    @property
    def guid(self):
        # type: () -> Union[str, None]
        if self.name:
            self._guid = str(int(hashlib.md5(self.name.encode()).hexdigest(),
                                 16) * -1)[:10]
        else:
            self._guid = None
        return self._guid

    def as_dict(self):
        # type: () -> dict
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

    def as_reference(self):
        # type: () -> dict
        d = {
            "typeName": self.type_name,
            "guid": self.guid
        }

        return d


class Cluster(Entity):
    attributes = ["name"]
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
        self._qualified_name = 'cluster://' + self.name


class SnowflakeDatabase(DataBase):
    type_name = "snowflake_database"
    attributes = ["name"]

    def __init__(self, name, parent, data=None):
        super(DataBase, self).__init__(name=name, data=data)
        self._qualified_name = parent.name + '://' + self.name
        self._data['cluster'] = parent.as_reference()


class SnowflakeSchema(Schema):
    type_name = "snowflake_schema"
    attributes = ["name"]

    def __init__(self, name, parent, data=None):
        super(Schema, self).__init__(name=name, data=data)
        self._qualified_name = parent.qualified_name + '.' + self.name
        self._data['database'] = parent.as_reference()


class SnowflakeTable(Table):
    type_name = "snowflake_table"
    attributes = ["name"]

    def __init__(self, name=None, table_alias=None, connection_id=None,
                 data=None):
        super(Table, self).__init__(name=name, data=data)
        if name:
            parent = self.create_parent_entities(table_alias, connection_id)
            self._qualified_name = parent.qualified_name + '/' + self.name
            self._data['parentSchema'] = parent.as_reference()

    def parse_alias(self, string):
        # type: (str) -> Tuple[str, str, str]
        account = string.split('/')[0]
        db = string.split('/')[1]
        schema = string.split('/')[2]
        return account, db, schema

    def create_parent_entities(self, table_alias, connection_id):
        # type: (Union[str, None], Union[str, None]) -> object
        if table_alias:
            account, db, schema = self.parse_alias(table_alias)
        self.account = SnowflakeAccount(account)
        self.db = SnowflakeDatabase(db, self.account)
        self.schema = SnowflakeSchema(schema, self.db)
        return self.schema

    # TODO: change function name
    def as_nested_dict(self):
        # type: () -> List[dict]
        d = self.as_dict()

        entities = []  # type: List[dict]
        entities.append(self.account.as_dict())
        entities.append(self.db.as_dict())
        entities.append(self.schema.as_dict())
        entities.append(d)

        return entities

    def as_dict(self):
        # type: () -> dict
        attributes = dict(self._data)
        attributes.update({"qualifiedName": self.qualified_name})

        env = Environment()
        if self.context:
            for key, value in six.iteritems(attributes):
                try:
                    attributes[key] = env.from_string(value).render(
                                                           **self.context)
                except Exception as e:  # noqa: F841
                    pass

        d = {
            "typeName": self.type_name,
            "attributes": attributes
        }
        return d


class Dag(Entity):
    type_name = "airflow_dag"
    attributes = ["name", "dag_id", "execution_date", "run_id"]


class Operator(DataSet):
    type_name = "airflow_operator"

    # todo we can derive this from the spec
    attributes = ["dag_id", "task_id", "command", "conn_id", "name",
                  "execution_date", "start_date", "end_date", "inputs",
                  "outputs", "airflow_dag"]
