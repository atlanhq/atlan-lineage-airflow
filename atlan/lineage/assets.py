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

DEFAULT_TENANT = ''


class Entity(object):
    attributes = []  # type: List[str]
    type_name = ""
    SOURCE_TYPE = ""

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
            self._guid = str(int(hashlib.md5(
                self.qualified_name.encode()).hexdigest(),
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


class Source(Entity):
    attributes = ["name", "sourceType", "type", "host", "port"]
    type_name = "AtlanSource"
    SOURCE_TYPE = ""

    def __init__(self, name=None, data=None, **kwargs):
        super(Source, self).__init__(name=name, data=data)


class DataBase(Entity):
    type_name = ""
    attributes = ["name"]
    SOURCE_TYPE = ""

    def __init__(self, name=None, data=None, **kwargs):
        super(Entity, self).__init__(name=name, data=data)


class Schema(Entity):
    type_name = ""
    attributes = ["name"]
    SOURCE_TYPE = ""

    def __init__(self, name=None, data=None, **kwargs):
        super(Entity, self).__init__(name=name, data=data)


class Table(DataSet):
    type_name = ""
    attributes = ["name"]
    SOURCE_TYPE = ""

    def __init__(self, name=None, data=None, **kwargs):
        super(DataSet, self).__init__(name=name, data=data)


class SnowflakeAccount(Source):
    type_name = "AtlanSource"
    attributes = ["name", "sourceType", "type", "host", "port"]
    SOURCE_TYPE = 'SNOWFLAKE'

    def __init__(self, name, data=None, **kwargs):
        super(Source, self).__init__(name=name, data=data,
                                     sourceType='SNOWFLAKE',
                                     type='snowflake', host=name)
        self._qualified_name = '{}:SNOWFLAKE://'.format(DEFAULT_TENANT) + self.name


class SnowflakeDatabase(DataBase):
    type_name = "AtlanDatabase"
    attributes = ["name", "sourceType"]
    SOURCE_TYPE = 'SNOWFLAKE'

    def __init__(self, name, parent, data=None, **kwargs):
        super(DataBase, self).__init__(name=name, data=data,
                                       sourceType='SNOWFLAKE')
        self._qualified_name = parent.qualified_name + '/' + self.name
        self._data['source'] = parent.as_reference()


class SnowflakeSchema(Schema):
    type_name = "AtlanSchema"
    attributes = ["name", "sourceType"]
    SOURCE_TYPE = 'SNOWFLAKE'

    def __init__(self, name, parent, source, data=None, **kwargs):
        super(Schema, self).__init__(name=name, data=data,
                                     sourceType='SNOWFLAKE')
        self._qualified_name = parent.qualified_name + '/' + self.name
        self._data['database'] = parent.as_reference()
        self._data['source'] = source.as_reference()


class SnowflakeTable(Table):
    type_name = "AtlanTable"
    attributes = ["name", "sourceType"]
    SOURCE_TYPE = 'SNOWFLAKE'

    def __init__(self, name=None, table_alias=None, connection_id=None,
                 data=None, **kwargs):
        super(Table, self).__init__(name=name, data=data,
                                    sourceType='SNOWFLAKE')
        if name:
            source, parent = self.create_parent_entities(table_alias, connection_id)  # noqa: E501
            self._qualified_name = parent.qualified_name + '/' + name
            self._data['atlanSchema'] = parent.as_reference()
            self._data['source'] = source.as_reference()

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
        self.db = SnowflakeDatabase(db, parent=self.account)
        self.schema = SnowflakeSchema(schema, parent=self.db, source=self.account)  # noqa: E501
        return self.account, self.schema

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


class AtlanJob(Entity):
    type_name = "AtlanJob"

    attributes = ["name", "description", "source", "extra", "schedule",
                  "jobCreatedAt", "jobUpdatedAt", "sourceType", "description"]


class AtlanJobRun(Entity):
    type_name = "AtlanJobRun"

    attributes = ["name", "description", "runId", "extra", "processes", "job",
                  "source", "runStatus", "runStartedAt", "runEndedAt",
                  "sourceType", "runType", "description"]


class AtlanProcess(DataSet):
    type_name = "AtlanProcess"

    attributes = ["name", "description", "processStartedAt", "processEndedAt",
                  "processStatus", "inputs", "outputs", "job_run", "extra",
                  "sourceType", "description"]
