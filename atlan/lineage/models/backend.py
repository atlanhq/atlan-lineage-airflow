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

from airflow.lineage.backend.atlas import AtlasBackend  # type: ignore

from typing import List, Dict
import copy

from airflow.utils.timezone import convert_to_utc  # type: ignore
from airflow.utils.log.logging_mixin import LoggingMixin  # type: ignore
from airflow.utils.net import get_hostname, get_host_ip_address  # type: ignore
from airflow.configuration import conf  # type: ignore

from atlan.lineage.assets import AtlanJobRun, AtlanProcess, AtlanJob, Source

import itertools  # noqa: F401

SERIALIZED_DATE_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"

_DAG_RUN_STATUS_MAP = {
    'running': "RUNNING",
    'success': "SUCCESS",
    'failed': "FAILED"
}

_TASK_RUN_STATUS_MAP = {
    'running': "RUNNING",
    'success': "SUCCESS",
    'failed': "FAILED"
}

log = LoggingMixin().log


class Backend(AtlasBackend):
    @staticmethod
    def create_lineage_meta(operator, inlets, outlets, context):
        _execution_date = convert_to_utc(context['ti'].execution_date)  # noqa
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

                outlet_list.append(entity_dict)
                outlet_ref_list.append({"typeName": entity.type_name,
                                        "uniqueAttributes": {
                                            "qualifiedName":
                                                entity.qualified_name
                                        }})

        # Creating dag and operator entities
        dag_op_list = []  # type: List[dict]

        # Creating source meta
        airflow_host = get_hostname()

        # qualified name format ':AIRFLOW//:{}'.format(airflow_host)
        data = {
            "qualifiedName": ':AIRFLOW//:{}'.format(airflow_host),
            "name": airflow_host,
            "host": get_host_ip_address(),
            "port": conf.get("webserver", "web_server_port"),
            "type": "airflow",
            "sourceType": "AIRFLOW",
            "typeName": "AtlanSource"
        }

        airflow_source = Source(data=data)
        log.info("Airflow Source: {}".format(airflow_source.as_dict()))
        dag_op_list.append(airflow_source.as_dict())

        # Creating dag meta
        qualified_name = "{}/{}".format(airflow_source.qualified_name,
                                        operator.dag_id)
        data = {
            "name": operator.dag_id,
            "source": airflow_source.as_reference(),
            "extra": Backend._get_dag_meta(context),
            "schedule": [{'cron': str(context['dag'].schedule_interval)}],
            "jobCreatedAt": None,
            "jobUpdatedAt": convert_to_utc(
                context['dag'].last_loaded).strftime(
                    SERIALIZED_DATE_FORMAT_STR),
            "sourceType": "AIRFLOW",
            "typeName": "AtlanJob"
        }

        if context['dag']._description:
            data["description"] = context['dag'].description

        dag = AtlanJob(qualified_name=qualified_name, data=data)
        log.info("Dag: {}".format(dag.as_dict()))
        dag_op_list.append(dag.as_dict())

        # Creating dag run meta
        qualified_name = "{}/{}".format(dag.qualified_name,
                                        context['dag_run'].run_id)

        assets = copy.deepcopy(inlet_ref_list)
        assets.extend(outlet_ref_list)

        data = {
            "name": operator.dag_id,
            "runId": context['dag_run'].run_id,
            "job": dag.as_reference(),
            "source": airflow_source.as_reference(),
            "runStatus": _DAG_RUN_STATUS_MAP.get(
                context['dag_run']._state, None),
            "extra": Backend._get_dag_run_meta(context),
            "sourceType": "AIRFLOW",
            "typeName": "AtlanJobRun"
        }

        if context['dag_run'].external_trigger:
            data.update({'runType': 'manual'})
        else:
            data.update({'runType': 'scheduled'})

        if len(assets) == 0:
            pass
        else:
            data.update({"assets": assets})

        if context['dag_run'].start_date:
            data['runStartedAt'] = convert_to_utc(
                context['dag_run'].start_date).strftime(
                    SERIALIZED_DATE_FORMAT_STR)
        if context['dag_run'].end_date:
            data['runEndedAt'] = convert_to_utc(
                context['dag_run'].end_date).strftime(
                    SERIALIZED_DATE_FORMAT_STR)

        dag_run = AtlanJobRun(qualified_name=qualified_name, data=data)
        log.info("Dag Run: {}".format(dag_run.as_dict()))

        dag_op_list.append(dag_run.as_dict())

        # Creating task meta
        operator_name = operator.__class__.__name__
        qualified_name = '{}/{}'.format(dag_run.qualified_name,
                                        operator.task_id)

        data = {
            "name": operator.task_id,
            "description": operator_name,
            "inputs": inlet_ref_list,
            "outputs": outlet_ref_list,
            "job_run": dag_run.as_reference(),
            "extra": Backend._get_task_meta(context),
            "processStatus": _TASK_RUN_STATUS_MAP.get(
                context['task_instance'].state, None),
            "sourceType": "AIRFLOW",
            "typeName": "AtlanProcess"
        }

        if _start_date:
            data["processStartedAt"] = _start_date.strftime(
                SERIALIZED_DATE_FORMAT_STR)
        if _end_date:
            data["processEndedAt"] = _end_date.strftime(
                SERIALIZED_DATE_FORMAT_STR)

        process = AtlanProcess(qualified_name=qualified_name, data=data)
        log.info("Process: {}".format(process.as_dict()))

        dag_op_list.append(process.as_dict())

        return inlet_list, outlet_list, dag_op_list

    @staticmethod
    def _get_dag_meta(context):
        context_keys_pop = ['END_DATE', 'conf', 'dag', 'dag_run', 'ds',  # noqa
        'ds_nodash', 'end_date', 'execution_date', 'inlets', 'latest_date',
        'macros', 'next_ds', 'next_ds_nodash', 'outlets', 'prev_ds',
        'prev_ds_nodash', 'run_id', 'task', 'task_instance',
        'task_instance_key_str', 'ti', 'tomorrow_ds', 'tomorrow_ds_nodash',
        'ts_nodash', 'ts_nodash_with_tz', 'yesterday_ds']

        dag_meta = context
        dag_meta_cleaned = {}  # type: Dict[str, str]
        dag_meta_cleaned.update(
            {
                'next_execution_date': str(dag_meta.get(
                    'next_execution_date', '')),
                'params': str(dag_meta.get('params', '')),
                'prev_execution_date': str(dag_meta.get(
                    'prev_execution_date', '')),
                'prev_execution_date_success': str(dag_meta.get(
                    'prev_execution_date_success', '')),
                'prev_start_date_success':  str(dag_meta.get(
                    'prev_start_date_success', '')),
                'tables': str(dag_meta.get('tables', '')),
                'templates_dict': str(dag_meta.get('templates_dict', '')),
                'test_mode': str(dag_meta.get('test_mode', '')),
                'ts': str(dag_meta.get('ts', '')),
                'var':  str(dag_meta.get('var', '')),

            }
        )
        dag_context = context['dag'].__dict__
        dag_meta_cleaned.update(
            {
                'access_control': str(dag_context.get('_access_control', '')),
                'default_view': str(dag_context.get('_default_view', '')),
                # 'description': str(dag_context.get('_description', '')),
                '_full_filepath': str(dag_context.get('_full_filepath', '')),
                '_old_context_manager_dags': str(dag_context.get(
                    '_old_context_manager_dags', [])),
                '_pickle_id': str(dag_context.get('_picke_id', '')),
                'catchup':  str(dag_context.get('catchup', '')),
                'dagrun_timeout':  str(dag_context.get('dagrun_timeout', '')),
                'owner':  str(dag_context.get('default_args', '').get(
                    'owner', '')),
                'start_date': str(dag_context.get('default_args', '').get(
                    'start_date', '')),
                'doc_md': str(dag_context.get('doc_md', '')),
                'end_date': str(dag_context.get('end_date', '')),
                'fileloc': str(dag_context.get('fileloc', '')),
                'is_paused_upon_creation': str(dag_context.get(
                    'is_paused_upon_creation')),
                'is_subdag': str(dag_context.get('is_subdag', '')),
                'jinja_environment_kwargs': str(dag_context.get(
                    'jinja_environment_kwargs', '')),
                # 'max_active_runs': 16,
                'on_failure_callback': str(dag_context.get(
                    'on_failure_callback', '')),
                'on_success_callback': str(dag_context.get(
                    'on_success_callback', '')),
                'orientation': str(dag_context.get('orientation', '')),
                'params': str(dag_context.get('params', {})),
                'parent_dag': str(dag_context.get('parent_dag', '')),
                'partial': str(dag_context.get('partial', '')),
                'safe_dag_id': str(dag_context.get('safe_dag_id', '')),
                'sla_miss_callback': str(dag_context.get(
                    'sla_miss_callback', '')),
                # 'start_date': None,
                'tags': str(dag_context.get('tags', '')),
                'task_count': str(dag_context.get('task_count', '')),
                'task_dict': str(dag_context.get('task_dict', '')),
                'template_searchpath': str(dag_context.get(
                    'template_searchpath', '')),
                # 'template_undefined':
                'timezone':  str(dag_context.get('timezone', '')),
                'user_defined_filters': str(dag_context.get(
                    'user_defined_filters', '')),
                'user_defined_macros': str(dag_context.get(
                    'user_defined_macros', ''))
            })

        return [dag_meta_cleaned]

    @staticmethod
    def _get_dag_run_meta(context):
        keys_excluded = []  # type: List # noqa

        meta = context['dag_run'].__dict__

        return [
            {
                'conf': str(meta.get('conf', '')),
                'external_trigger': str(meta.get('external_trigger', '')),
                'airflow_id': str(meta.get('id', '')),
                'execution_date': str(meta.get('execution_date', ''))
            }
        ]

    @staticmethod
    def _get_task_meta(context):
        keys_excluded = ['_log', 'sa_instance_state', 'dag_id']

        # context['ti'] and context['task_instance']
        # are exactly the same
        meta = context['task_instance'].__dict__

        task_meta = {
                        'try_number': str(meta.get('_try_number', '')),
                        'duration': str(meta.get('duration', '')),
                        'execution_date': str(meta.get('execution_date', '')),
                        'executor_config': str(meta.get(
                            'executor_config', {})),
                        'hostname': str(meta.get('hostname', '')),
                        'job_id': str(meta.get('job_id', '')),
                        'max_tries': str(meta.get('max_tries', '')),
                        'pid': str(meta.get('pid', '')),
                        'pool': str(meta.get('pool', '')),
                        'pool_slots': str(meta.get('pool_slots', '')),
                        'priority_weight': str(meta.get(
                            'priority_weight', '')),
                        'queue': str(meta.get('queue', '')),
                        'queued_dttm': str(meta.get('queued_dttm', '')),
                        'raw': str(meta.get('raw', '')),
                        'run_as_user': str(meta.get('run_as_user', '')),
                        'test_mode': str(meta.get('test_mode', '')),
                        'unixname': str(meta.get('unixname', ''))
                    }

        try:
            task_details = meta.get('task').__dict__

            keys_excluded = ['task_id', 'owner', 'start_date', 'end_date',
                             '_schedule_interval', 'pool', 'pool_slots',
                             'priority_weight', 'run_as_user',
                             'task_concurrency', '_dag', 'inlets', 'outlets',
                             'lineage_data', '_inlets', '_outlets',
                             'op_kwargs', '_log']

            task_config = {}  # type: dict

            for k, v in task_details.items():
                if k not in keys_excluded:
                    task_config[k] = v

            keys_excluded = ['conf', 'dag', 'ds', 'next_ds', 'next_ds_nodash',
                             'prev_ds', 'prev_ds_nodash', 'ds_nodash', 'ts',
                             'ts_nodash', 'ts_nodash_with_tz', 'yesterday_ds',
                             'yesterday_ds_nodash', 'tomorrow_ds',
                             'tomorrow_ds_nodash', 'END_DATE', 'end_date',
                             'dag_run', 'run_id', 'execution_date',
                             'prev_execution_date', 'prev_execution_date_success',  # noqa: E501
                             'prev_start_date_success', 'next_execution_date',
                             'latest_date', 'macros', 'params', 'tables',
                             'task', 'task_instance', 'ti', 'test_mode',
                             'task_instance_key_str', 'var', 'inlets',
                             'outlets', 'templates_dict']

            task_op_kwargs = {}

            for k, v in task_details['op_kwargs'].items():
                if k not in keys_excluded:
                    task_op_kwargs[k] = v

            task_config['op_kwargs'] = task_op_kwargs

            task_meta.update({'task': str(task_config)})
        except Exception as e:  # noqa: F841
            pass

        return [task_meta]

    def _get_source_meta(context):
        conf = context['conf']
        meta = {}
        for k, v in conf.items():
            try:
                meta[k] = dict(v)
            except Exception as e:  # noqa: F841
                meta[k] = str(v)

        return [meta]
