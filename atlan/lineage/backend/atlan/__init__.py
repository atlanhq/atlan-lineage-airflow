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
from typing import List, Any, Union, NoReturn  # noqa: F401
import requests
import json

from airflow.configuration import conf  # type: ignore
from airflow.utils.log.logging_mixin import LoggingMixin  # type: ignore

from atlan.lineage.models.backend import Backend

log = LoggingMixin().log

try:
    _url = conf.get("atlan", "url")
    _token = conf.get("atlan", "token")
except:
    _url = ""
    _token = ""

_headers = {
  'token': _token,
  'Content-Type': 'application/json'
}


def create_bulk(data):
    # type: (List[dict]) -> Union[NoReturn, None]
    try:
        url = "https://{url}/api/v1/metadata/entities/bulk".format(url=_url)
        payload = json.dumps(data)
        response = requests.request("POST", url, headers=_headers,
                                    data=payload)
        if not response.status_code == 200:
            message = "API call failed with response code {}. Error message: \
                                {}".format(response.status_code, response.text)
            raise Exception(message)
        else:
            return None
    except Exception as e:
        raise Exception(e)


def create(data):
    # type: (dict) -> Union[NoReturn, None]
    try:
        url = "https://{url}/api/v1/metadata/entities".format(url=_url)
        payload = json.dumps(data)
        response = requests.request("POST", url, headers=_headers,
                                    data=payload)
        if not response.status_code == 200:
            message = "API call failed with response code {}. Error message: \
                                {}".format(response.status_code, response.text)
            raise Exception(message)
        else:
            return None
    except Exception as e:
        raise Exception(e)


class AtlanBackend(Backend):
    @staticmethod
    def send_lineage(operator, inlets, outlets, context):
        # type: (object, list, list, dict) -> None

        inlet_list, outlet_list, dag_op_list = Backend.create_lineage_meta(
                                                                    operator,
                                                                    inlets,
                                                                    outlets,
                                                                    context)

        if inlets:
            for entity_dict in inlet_list:
                if entity_dict is None:
                    continue

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
                    log.info("Failed to create inlets. Error: {}".format(e))

        if outlets:
            for entity_dict in outlet_list:
                if not entity_dict:
                    continue

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
                    log.info("Failed to create outlets. Error: {}".format(e))

        log.info("Creating dag and operator entities")
        create_bulk(data=dag_op_list)
        log.info("Done. Created lineage")
