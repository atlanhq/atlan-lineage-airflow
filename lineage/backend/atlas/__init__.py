from airflow.configuration import conf
from atlan_lite.lineage import datasets
from airflow.lineage.backend.atlas import AtlasBackend
from airflow.utils.timezone import convert_to_utc

from atlasclient.client import Atlas
from atlasclient.exceptions import HttpError

SERIALIZED_DATE_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"

_username = conf.get("atlas", "username")
_password = conf.get("atlas", "password")
_port = conf.get("atlas", "port")
_host = conf.get("atlas", "host")


class AtlanBackend(AtlasBackend):
    @staticmethod
    def send_lineage(operator, inlets, outlets, context):
        print("IN SEND LINEAGE")
        client = Atlas(_host, port=_port, username=_username, password=_password)

        _execution_date = convert_to_utc(context['ti'].execution_date)
        _start_date = convert_to_utc(context['ti'].start_date)
        _end_date = convert_to_utc(context['ti'].end_date)

        inlet_list = []
        if inlets:
            for entity in inlets:
                if entity is None:
                    continue

                entity.set_context(context)
                print("ENTITY:", entity.as_dict())
                client.entity_post.create(data={"entity": entity.as_dict()})
                print("THIS IS RUNNING")
                inlet_list.append({"typeName": entity.type_name,
                                   "uniqueAttributes": {
                                       "qualifiedName": entity.qualified_name
                                   }})


        print("CUSTOM MODULE INLETS: ", inlet_list)
        outlet_list = []
        if outlets:
            for entity in outlets:
                if not entity:
                    continue

                entity.set_context(context)
                client.entity_post.create(data={"entity": entity.as_dict()})
                print("THIS IS RUNNING")
                outlet_list.append({"typeName": entity.type_name,
                                    "uniqueAttributes": {
                                        "qualifiedName": entity.qualified_name
                                    }})

        print("CUSTOM MODULE OUTLETS: ", outlet_list)
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

        process = datasets.ETLOperator(qualified_name=qualified_name, data=data)
        print("THIS IS NOT RUNNING")
        print("PROCESS CUSTOM MODULE")
        print(process.as_dict())

        client.entity_post.create(data={"entity": process.as_dict()})
