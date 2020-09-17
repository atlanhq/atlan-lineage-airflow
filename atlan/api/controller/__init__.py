from flask import request  # noqa
import json

from atlan.rest.utils import DagDto

api = DagDto.api
_dags = DagDto.dags


@api.route("/dags", methods=['GET'])
def dags(self):
    """List all registered users"""
    return json.dumps({'status': True})
