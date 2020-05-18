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

from airflow.lineage.backend.atlas import AtlasBackend

from typing import List

from airflow.utils.timezone import convert_to_utc
from airflow.utils.log.logging_mixin import LoggingMixin

from atlan.lineage.assets import Dag, Operator

import itertools  # noqa: F401

SERIALIZED_DATE_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"

log = LoggingMixin().log


class Backend(AtlasBackend):
    @staticmethod
    def create_lineage_meta(operator, inlets, outlets, context):
        # type: (object, list, list, dict) -> list, list, list
        _execution_date = convert_to_utc(context['ti'].execution_date)
        _start_date = convert_to_utc(context['ti'].start_date)
        _end_date = convert_to_utc(context['ti'].end_date)

        # Creating input entities
        inlet_list = []  # type: List[dict]
        inlet_ref_list = []  # type: List[dict]
        if inlets:
            for entity in inlets:
                if entity is None:
                    continue

                entity.set_context(context)
                try:
                    entity_dict = entity.as_nested_dict()
                except Exception as e:  # noqa: F841
                    entity_dict = entity.as_dict()

                # log.info("Inlets: {}".format(entity_dict))
                # log.info("Creating input entities")
                # try:
                #     if isinstance(entity_dict, dict):
                #         log.info("Calling the single entity create API")
                #         create(data=entity_dict)
                #     elif isinstance(entity_dict, list):
                #         log.info("Calling the bulk entity create API")
                #         create_bulk(data=entity_dict)
                # except Exception as e:
                #     log.info("Error creating inlet entity. \
                # Error: {}".format(e))

                inlet_list.append(entity_dict)
                inlet_ref_list.append({"typeName": entity.type_name,
                                       "uniqueAttributes": {
                                           "qualifiedName":
                                               entity.qualified_name
                                               }
                                       })

        # Creating output entities
        outlet_list = []  # type: List[dict]
        outlet_ref_list = []  # type: List[dict]
        if outlets:
            for entity in outlets:
                if not entity:
                    continue

                entity.set_context(context)
                try:
                    entity_dict = entity.as_nested_dict()
                except Exception as e:  # noqa: F841
                    entity_dict = entity.as_dict()

                log.info("Outlets: {}".format(entity_dict))
                # log.info("Creating output entities")
                # try:
                #     if isinstance(entity_dict, dict):
                #         log.info("Calling the single entity create API")
                #         create(data=entity_dict)
                #     elif isinstance(entity_dict, list):
                #         log.info("Calling the bulk entity create API")
                #         create_bulk(data=entity_dict)
                # except Exception as e:
                #     log.info("Error creating outlet entity. \
                #   Error: {}".format(e))

                outlet_list.append(entity_dict)
                outlet_ref_list.append({"typeName": entity.type_name,
                                        "uniqueAttributes": {
                                            "qualifiedName":
                                                entity.qualified_name
                                        }})

        # Creating dag and operator entities
        dag_op_list = []  # type: List[dict]

        dag_name = "{}".format(operator.dag_id)
        qualified_name = "{}_{}".format(operator.dag_id, _execution_date)
        data = {
            "dag_id": operator.dag_id,
            "execution_date": _execution_date.strftime(
                                        SERIALIZED_DATE_FORMAT_STR),
            "name": dag_name,
            "run_id": context['dag_run'].run_id
        }
        dag = Dag(qualified_name=qualified_name, data=data)
        log.info("Dag: {}".format(dag.as_dict()))

        dag_op_list.append(dag.as_dict())

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
            "inputs": inlet_ref_list,
            "outputs": outlet_ref_list,
            "command": operator.lineage_data,
            "airflow_dag": dag.as_reference()
        }

        if _start_date:
            data["start_date"] = _start_date.strftime(
                                        SERIALIZED_DATE_FORMAT_STR)
        if _end_date:
            data["end_date"] = _end_date.strftime(SERIALIZED_DATE_FORMAT_STR)

        process = Operator(qualified_name=qualified_name, data=data)
        log.info("Process: {}".format(process.as_dict()))

        # log.info("Creating process entity")
        # create(data=process.as_dict())
        # log.info("Done. Created lineage")
        dag_op_list.append(process.as_dict())

        return inlet_list, outlet_list, dag_op_list
