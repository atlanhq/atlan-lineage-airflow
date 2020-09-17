import json
import os
import six
import time
import pickle

from datetime import datetime
from datetime import timezone
import dateutil.parser

from flask import Blueprint, request, Response
from sqlalchemy import or_, and_
from airflow import settings
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.models import DagBag, DagRun, DagModel, TaskInstance, XCom
from airflow.utils.state import State
from airflow.utils.dates import date_range as utils_date_range
from airflow.www.app import csrf

airflow_api_blueprint = Blueprint('airflow_api', __name__, url_prefix='/api/atlan/v1')

# TODO: add white space stripping 

class ApiInputException(Exception):
    pass


class ApiResponse:

    def __init__(self):
        pass

    STATUS_OK = 200
    STATUS_BAD_REQUEST = 400
    STATUS_UNAUTHORIZED = 401
    STATUS_NOT_FOUND = 404
    STATUS_SERVER_ERROR = 500

    @staticmethod
    def standard_response(status, payload):
        json_data = json.dumps({
            'response': payload
        })
        resp = Response(json_data, status=status, mimetype='application/json')
        return resp

    @staticmethod
    def success(payload):
        return ApiResponse.standard_response(ApiResponse.STATUS_OK, payload)

    @staticmethod
    def error(status, error):
        return ApiResponse.standard_response(status, {
            'error': error
        })

    @staticmethod
    def bad_request(error):
        return ApiResponse.error(ApiResponse.STATUS_BAD_REQUEST, error)

    @staticmethod
    def not_found(error='Resource not found'):
        return ApiResponse.error(ApiResponse.STATUS_NOT_FOUND, error)

    @staticmethod
    def unauthorized(error='Not authorized to access this resource'):
        return ApiResponse.error(ApiResponse.STATUS_UNAUTHORIZED, error)

    @staticmethod
    def server_error(error='An unexpected problem occurred'):
        return ApiResponse.error(ApiResponse.STATUS_SERVER_ERROR, error)

@airflow_api_blueprint.before_request
def verify_authentication():
    authorization = request.headers.get('authorization')
    try:
        api_auth_key = 'auth'  #settings.conf.get('AIRFLOW_API_PLUGIN', 'AIRFLOW_API_AUTH')
    except AirflowConfigException:
        return

    if authorization != api_auth_key:
        return ApiResponse.unauthorized("You are not authorized to use this resource")


def format_dag_run(dag_run):
    return {
        'run_id': dag_run.run_id,
        'dag_id': dag_run.dag_id,
        'state': dag_run.get_state(),
        'start_date': (None if not dag_run.start_date else str(dag_run.start_date)),
        'end_date': (None if not dag_run.end_date else str(dag_run.end_date)),
        'external_trigger': dag_run.external_trigger,
        'execution_date': str(dag_run.execution_date)
    }


def find_dag_runs(session, dag_id, dag_run_id, execution_date):
    qry = session.query(DagRun)
    qry = qry.filter(DagRun.dag_id == dag_id)
    qry = qry.filter(or_(DagRun.run_id == dag_run_id, DagRun.execution_date == execution_date))

    return qry.order_by(DagRun.execution_date).all()


# @airflow_api_blueprint.route('/', methods=['GET'])
# def dags_index():
#     dagbag = DagBag('dags')
#     dags = []
#     for dag_id in dagbag.dags:
#         payload = {
#             'dag_id': dag_id,
#             'full_path': None,
#             'is_active': False,
#             'last_execution': None,
#         }

#         dag = dagbag.get_dag(dag_id)

#         if dag:
#             payload['full_path'] = dag.full_filepath
#             payload['is_active'] = (not dag.is_paused)
#             payload['last_execution'] = str(dag.latest_execution_date)

#         dags.append(payload)

#     return ApiResponse.success({'dags': dags})


@csrf.exempt
@airflow_api_blueprint.route('/dags', methods=['GET'])
# query params: list of dag name, all
def dags():

    session = settings.Session()

    query = session.query(DagModel)

    dags = request.args.get('dags')
    if dags:
        dags = dags.split(',')
        query = query.filter(DagModel.dag_id.in_(dags))

    result = query.all()

    dags = []
    for r in result:
        dags.append({
            'dag_id': r.dag_id,
            'is_paused': r.is_paused,
            'is_subdag': r.is_subdag,
            'is_active': r.is_active,
            'last_scheduler_run': r.last_scheduler_run.isoformat(),
            'last_pickled': ((r.last_pickled or '') and
                             r.last_pickled.isoformat()),
            'last_expired': ((r.last_expired or '') and
                             r.last_expired.isoformat()),
            'scheduler_lock': r.scheduler_lock,
            'pickle_id': r.pickle_id,
            'fileloc': r.fileloc,
            'owners': r.owners,
            'description': r.description,
            'default_view': r.default_view,
            'schedule_interval': str(r.schedule_interval),
            'root_dag_id': r.root_dag_id
        })

    session.close()

    return ApiResponse.success({'dags': dags})

@csrf.exempt
@airflow_api_blueprint.route('/dag_runs', methods=['GET'])
# request params: between time period, dag id
def dag_runs():
    dag_runs = []

    session = settings.Session()

    query = session.query(DagRun)

    dag_id = request.args.get('dag_id')
    if dag_id:
        # dags = dags.split(',')
        query = query.filter(DagRun.dag_id == dag_id)

    before_timestamp = request.args.get('before_timestamp')
    if before_timestamp:
        print("EBFORE")
        date = before_timestamp
        # date = datetime.strptime(date, '%Y-%m-%d')
        date = dateutil.parser.parse(date)
        date = date.replace(tzinfo=timezone.utc)
        query = query.filter(DagRun.execution_date <= date)

    after_timestamp = request.args.get('after_timestamp')
    if after_timestamp:
        print("AFTER")
        date = after_timestamp
        # date = datetime.strptime(date, '%Y-%m-%d')
        date = dateutil.parser.parse(date)
        date = date.replace(tzinfo=timezone.utc)
        query = query.filter(DagRun.execution_date >= date)

    result = query.all()
    for r in result:
        if r.conf is not None:
            try:
                # print('DEBUG', r.dag_id, r.conf, type(r.conf) )
                conf = r.conf #json.loads(r.conf.decode('UTF-8'))
            except (UnicodeEncodeError, ValueError):
                # For backward-compatibility.
                # Preventing errors in webserver
                # due to XComs mixed with pickled and unpickled.
                conf = pickle.loads(r.conf)
        else:
            conf = None
        dag_runs.append({
            'id': r.id,
            'run_id': r.run_id,
            'state': r.state,
            'dag_id': r.dag_id,
            'execution_date': r.execution_date.isoformat(),
            'start_date': ((r.start_date or '') and
                           r.start_date.isoformat()),
            'end_date': ((r.end_date or '') and
                          r.end_date.isoformat()),
            'conf': conf
        })

    session.close()
    return ApiResponse.success({'dag_runs': dag_runs})

@csrf.exempt
@airflow_api_blueprint.route('/tasks', methods=['GET'])
# request params: execution_date, dag id
def tasks():
    tasks = []

    session = settings.Session()

    query = session.query(TaskInstance)

    dag_id = request.args.get('dag_id')
    execution_date = request.args.get('execution_date')

    date = execution_date
    date = dateutil.parser.parse(date)

    query = query.filter(and_(TaskInstance.execution_date >= date, TaskInstance.dag_id == dag_id))

    result = query.all()
    for r in result:
        if r.executor_config is not None:
            try:
                executor_config = r.executor_config   # json.loads(r.executor_config.decode('UTF-8'))
            except (UnicodeEncodeError, ValueError):
                executor_config = pickle.loads(r.executor_config)
        else:
            executor_config = None
        tasks.append({
            'task_id': r.task_id,
            'dag_id': r.dag_id,
            'state': r.state,
            'duration': r.duration,
            'try_number': r.try_number,
            'hostname': r.hostname,
            'unixname': r.unixname,
            'job_id': r.job_id,
            'pool': r.pool,
            'queue': r.queue,
            'priority_weight': r.priority_weight,
            'operator': r.operator,
            'max_tries': r.max_tries,
            'pool_slots': r.pool_slots,
            'execution_date': r.execution_date.isoformat(),
            'start_date': ((r.start_date or '') and
                           r.start_date.isoformat()),
            'end_date': ((r.end_date or '') and
                           r.end_date.isoformat()),
            'executor_config': executor_config,
            'queued_dttm': ((r.queued_dttm or '') and
                           r.queued_dttm.isoformat())
        })

    session.close()
    return ApiResponse.success({'tasks': tasks})

@csrf.exempt
@airflow_api_blueprint.route('/xcoms', methods=['GET'])
# request params: execution_date, dag id
def xcoms():
    xcoms = []

    session = settings.Session()

    query = session.query(XCom)

    dag_id = request.args.get('dag_id')
    execution_date = request.args.get('execution_date')

    date = execution_date
    date = dateutil.parser.parse(date)

    xcom_keys = ['pipeline_inlets', 'pipeline_outlets']

    query = query.filter(and_(XCom.execution_date == date, XCom.dag_id == dag_id, XCom.key.in_(xcom_keys)))

    result = query.all()
    for r in result:
        if r.value is not None:
            try:
                value = r.value# json.loads(r.value.decode('UTF-8'))
            except (UnicodeEncodeError, ValueError):
                value = pickle.loads(r.value)
        else:
            value = None
        xcoms.append({
            'id': r.id,
            'key': r.key,
            'value': value,
            'timestamp': r.timestamp.isoformat(),
            'task_id': r.task_id,
            'dag_id': r.dag_id,
            'execution_date': r.execution_date.isoformat(),

        })

    session.close()
    return ApiResponse.success({'xcoms': xcoms})
