from flask import Namespace, fields  # noqa


class DagDto:
    api = Namespace('dags', description='Get all DAGs')
    dags = api.model('dags', {})