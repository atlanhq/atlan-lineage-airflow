import six

from typing import List
from jinja2 import Environment

from airflow.lineage.datasets import * 
from airflow.lineage.datasets import _inherited

class ETLOperator(Asset):
    type_name = "etl_operator2"

    # todo we can derive this from the spec
    attributes = ["dag_id", "task_id", "command", "conn_id", "name", "execution_date",
                  "start_date", "end_date", "inputs", "outputs"]

class Asset(object):
    type_name = "Asset"
    attributes = []

    # def __init__(self, name=None, data=None, qualified_name = None):
    #     print("NAME:", name)
    #     super(Table, self).__init__(name=name, data=data)

    #     self._qualified_name = qualified_name
    #     # self._data['path'] = self.name

    def __init__(self, name = None, qualified_name=None, data=None, **kwargs):
        self._qualified_name = qualified_name
        self.name = name
        self.context = None
        self._data = dict()

        self._data.update({'name':name})
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

    def __getattr__(self, attr):
        if attr in self.attributes:
            if self.context:
                env = Environment()
                return env.from_string(self._data.get(attr)).render(**self.context)

            return self._data.get(attr)

        raise AttributeError(attr)

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __iter__(self):
        for key, value in six.iteritems(self._data):
            yield (key, value)

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
        }
        print("CUSTOMMMM  AS DICT:", d)

        return d

    @staticmethod
    def map_type(name):
        for cls in _inherited(DataSet):
            if cls.type_name == name:
                return cls

        raise NotImplementedError("No known mapping for {}".format(name))

class Table(Asset):
    type_name = "table"
    attributes = ["name"]

    def __init__(self, data=None, qualified_name = None):
        name = qualified_name.split("/")[-1]
        super(Table, self).__init__(name=name, data=data)

        self._qualified_name = qualified_name
        # self._data['path'] = self.name

