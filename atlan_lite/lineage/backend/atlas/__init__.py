from typing import List, Union

from airflow.configuration import conf
from airflow.utils.timezone import convert_to_utc
from airflow.utils.log.logging_mixin import LoggingMixin

from atlasclient.client import Atlas
from atlasclient.exceptions import HttpError

from atlan_lite.lineage.models.backend import Backend
from atlan_lite.lineage.backend.atlas.typedefs import operator_typedef, entity_typedef
# from airflow.lineage import datasets
from atlan_lite.lineage.assets import Dag, Operator

import itertools

SERIALIZED_DATE_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"

_username = conf.get("atlas", "username")
_password = conf.get("atlas", "password")
_port = conf.get("atlas", "port")
_host = conf.get("atlas", "host")

log = LoggingMixin().log


class AtlasBackend(Backend):
    @staticmethod
    def send_lineage(operator, inlets, outlets, context):
        # type: (object, List[dict], List[dict], dict) -> None

        print("CONTEXT:", context)
        client = Atlas(_host,
                       port=_port,
                       username=_username,
                       password=_password)

        try:
            log.info("Creating operator type on Atlas")
            client.typedefs.create(data=operator_typedef)
        except HttpError:
            log.info("Operator type already present on Atlas, updating type")
            client.typedefs.update(data=operator_typedef)

        try:
            log.info("Creating snowflake types on Atlas")
            client.typedefs.create(data=entity_typedef)
        except HttpError:
            log.info("Snowflake types already present on Atlas, updating types")
            client.typedefs.update(data=entity_typedef)

        inlet_list, outlet_list, dag_op_list = Backend.create_lineage_meta(operator, inlets, outlets, context)

        if inlets:
            for entity_dict in inlet_list:
                if entity_dict is None:
                    continue

                log.info("Inlets: {}".format(entity_dict))
                log.info("Creating input entities")
                try:
                    if isinstance(entity_dict, dict):
                        client.entity_post.create(data={"entity": entity_dict})
                    elif isinstance(entity_dict, list):
                        client.entity_bulk.create(data={"entities": entity_dict})
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
                        client.entity_post.create(data={"entity": entity_dict})
                    elif isinstance(entity_dict, list):
                        client.entity_bulk.create(data={"entities": entity_dict})
                except Exception as e:
                    log.info("Failed to create outlets. Error: {}".format(e))

        
        
        log.info("Creating dag and operator entities")
        # client.entity_post.create(data={"entity": process.as_dict()})
        client.entity_bulk.create(data={"entities": dag_op_list})
        log.info("Done. Created lineage")



