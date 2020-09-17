from airflow.plugins_manager import AirflowPlugin

from atlan.api.blueprints import airflow_api_blueprint

# from flask_restful import Api, Resource
# from flask import Blueprint

# from atlan.rest.controller import api as ns


# blueprint = Blueprint('api', __name__, url_prefix="/api")

# api = Api(blueprint)

# # Add the namespaces to API
# api.add_resource(ns)


class AtlanRESTApiPlugin(AirflowPlugin):
    name = "atlan_airflow_api"

    flask_blueprints = [
        airflow_api_blueprint
    ]
