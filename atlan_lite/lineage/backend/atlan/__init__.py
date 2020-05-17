from typing import List, Any, Union, NoReturn
import requests
import json

from airflow.configuration import conf
from airflow.utils.timezone import convert_to_utc
from airflow.utils.log.logging_mixin import LoggingMixin

from atlan_lite.models.backend import Backend
from airflow.lineage import datasets

SERIALIZED_DATE_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"

log = LoggingMixin().log

_url = conf.get("atlan", "url")
_token = conf.get("atlan", "token")

_headers = {
  'token': _token,
  'Content-Type': 'application/json'
}

def create_bulk(data):
    # type: (List[dict]) -> Union[NoReturn, None]
    try:
        url = "https://{url}/entities/bulk".format(url=_url)
        payload = json.dumps(data)
        response = requests.request("POST", url, headers=_headers, data=payload)
        if not response.status_code == 200:
            message = "API call failed with response code {}. Error message: {}".format(response.status_code, response.text)
            raise Exception(message)
        else:
            return None
    except Exception as e:
        raise Exception(e)

def create(data):
    # type: (dict) -> Union[NoReturn, None]
    try:
        url = "https://{url}/entities".format(url=_url)
        payload = json.dumps(data)
        response = requests.request("POST", url, headers=_headers, data=payload)
        if not response.status_code == 200:
            message = "API call failed with response code {}. Error message: {}".format(response.status_code, response.text)
            raise Exception(message)
        else:
            return None
    except Exception as e:
        raise Exception(e)



class AtlanBackend(Backend):
    @staticmethod
    def send_lineage(operator, inlets, outlets, context):
        # type: (Any, list, list, dict) -> None

        _execution_date = convert_to_utc(context['ti'].execution_date)
        _start_date = convert_to_utc(context['ti'].start_date)
        _end_date = convert_to_utc(context['ti'].end_date)

        inlet_list = []  # type: List[dict]
        if inlets:
            for entity in inlets:
                if entity is None:
                    continue

                entity.set_context(context)
                try:
                    entity_dict = entity.as_nested_dict()
                except Exception as e:
                    entity_dict = entity.as_dict()

                log.info("Inlets: {}".format(entity_dict))
                log.info("Creating input entities")
                try:
                    if isinstance(entity_dict, dict):
                        log.info("Calling the single entity create API")
                        create(data=entity_dict)
                    elif isinstance(entity_dict, list):
                        log.info("Calling the bulk entity create API")
                        create_bulk(data=entity_dict)
                except Exception as e:
                    log.info("Error creating inlet entity. Error: {}".format(e))

                inlet_list.append({"typeName": entity.type_name,
                                   "uniqueAttributes": {
                                       "qualifiedName": entity.qualified_name
                                        }
                                   })

        outlet_list = []  # type: List[dict]
        if outlets:
            for entity in outlets:
                if not entity:
                    continue

                entity.set_context(context)
                try:
                    entity_dict = entity.as_nested_dict()
                except Exception as e:
                    entity_dict = entity.as_dict()

                log.info("Outlets: {}".format(entity_dict))
                log.info("Creating output entities")
                try:
                    if isinstance(entity_dict, dict):
                        log.info("Calling the single entity create API")
                        create(data=entity_dict)
                    elif isinstance(entity_dict, list):
                        log.info("Calling the bulk entity create API")
                        create_bulk(data=entity_dict)
                except Exception as e:
                    log.info("Error creating outlet entity. Error: {}".format(e))

                outlet_list.append({"typeName": entity.type_name,
                                    "uniqueAttributes": {
                                        "qualifiedName": entity.qualified_name
                                    }})

        operator_name = operator.__class__.__name__
        name = "{}".format(operator.task_id)
        qualified_name = "{}_{}_{}@{}".format(operator.dag_id,
                                              operator.task_id,
                                              _execution_date,
                                              operator_name)

        data = {
            "dag_id": operator.dag_id,
            "task_id": operator.task_id,
            "execution_date": _execution_date.strftime(
                                        SERIALIZED_DATE_FORMAT_STR),
            "name": name,
            "inputs": inlet_list,
            "outputs": outlet_list,
            "command": operator.lineage_data,
        }

        if _start_date:
            data["start_date"] = _start_date.strftime(
                                        SERIALIZED_DATE_FORMAT_STR)
        if _end_date:
            data["end_date"] = _end_date.strftime(SERIALIZED_DATE_FORMAT_STR)

        process = datasets.Operator(qualified_name=qualified_name, data=data)
        log.info("Process: {}".format(process.as_dict()))

        log.info("Creating process entity")
        create(data=process.as_dict())
        log.info("Done. Created lineage")
