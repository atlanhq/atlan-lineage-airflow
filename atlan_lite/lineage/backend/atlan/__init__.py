from airflow.configuration import conf
from airflow.utils.timezone import convert_to_utc

from atlasclient.client import Atlas
from atlasclient.exceptions import HttpError

from atlan_lite.models.backend import Backend
from atlan_lite.lineage.backend.atlan.typedefs import operator_typedef, entity_typedef
from airflow.lineage import datasets

import itertools

SERIALIZED_DATE_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"

_username = conf.get("atlas", "username")
_password = conf.get("atlas", "password")
_port = conf.get("atlas", "port")
_host = conf.get("atlas", "host")


class AtlasBackend(Backend):
    @staticmethod
    def send_lineage(operator, inlets, outlets, context):
        client = Atlas(_host, port=_port, username=_username, password=_password)

        try:
            client.typedefs.create(data=operator_typedef)
        except HttpError:
            client.typedefs.update(data=operator_typedef)

        try:
            client.typedefs.create(data=entity_typedef)
        except HttpError:
            client.typedefs.update(data=entity_typedef)

        _execution_date = convert_to_utc(context['ti'].execution_date)
        _start_date = convert_to_utc(context['ti'].start_date)
        _end_date = convert_to_utc(context['ti'].end_date)

        inlet_list = []
        if inlets:
            for entity in inlets:
                if entity is None:
                    continue

                entity.set_context(context)
                print("INLET:", entity.as_dict())
                entity_dict = entity.as_dict()
                try:
                    if isinstance(entity_dict, dict):
                        client.entity_post.create(data={"entity": entity_dict})
                    elif isinstance(entity_dict, list):
                        client.entity_bulk.create(data={"entities": entity_dict})
                except Exception as e:
                    print(e)
            
                inlet_list.append({"typeName": entity.type_name,
                                "uniqueAttributes": {
                                    "qualifiedName": entity.qualified_name
                                }})


        outlet_list = []
        if outlets:
            for entity in outlets:
                if not entity:
                    continue

                entity.set_context(context)
                print("OUTLET:", entity.as_dict())
                entity_dict = entity.as_dict()
                try:
                    if isinstance(entity_dict, dict):
                        client.entity_post.create(data={"entity": entity_dict})
                    elif isinstance(entity_dict, list):
                        client.entity_bulk.create(data={"entities": entity_dict})
                except Exception as e:
                    print(e)

                outlet_list.append({"typeName": entity.type_name,
                                    "uniqueAttributes": {
                                        "qualifiedName": entity.qualified_name
                                    }})

        operator_name = operator.__class__.__name__
        name = "{} {} ({})".format(operator.dag_id, operator.task_id, operator_name)
        qualified_name = "{}_{}_{}@{}".format(operator.dag_id,
                                              operator.task_id,
                                              _execution_date,
                                              operator_name)

        data = {
            "dag_id": operator.dag_id,
            "task_id": operator.task_id,
            "execution_date": _execution_date.strftime(SERIALIZED_DATE_FORMAT_STR),
            "name": name,
            "inputs": inlet_list,
            "outputs": outlet_list,
            "command": operator.lineage_data,
        }

        if _start_date:
            data["start_date"] = _start_date.strftime(SERIALIZED_DATE_FORMAT_STR)
        if _end_date:
            data["end_date"] = _end_date.strftime(SERIALIZED_DATE_FORMAT_STR)

        process = datasets.Operator(qualified_name=qualified_name, data=data)
        print("PROCESS: ", process.as_dict())
        
        client.entity_post.create(data={"entity": process.as_dict()})
