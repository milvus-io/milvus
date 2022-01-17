from __future__ import print_function
from utils import *
import logging
from pprint import pprint
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from milvus_benchmark import config as cf

logger = logging.getLogger("milvus_benchmark.chaos.chaosOpt")

class ChaosOpt(object):
    def __init__(self, kind, group=cf.DEFAULT_GROUP, version=cf.DEFAULT_VERSION, namespace=cf.CHAOS_NAMESPACE):
        self.group = group
        self.version = version
        self.namespace = namespace
        self.plural = kind.lower()

    # def get_metadata_name(self):
    #     return self.metadata_name

    def create_chaos_object(self, body):
        # body = create_chaos_config(self.plural, self.metadata_name, spec_params)
        # logger.info(body)
        pretty = 'true'
        config.load_kube_config()
        api_instance = client.CustomObjectsApi()
        try:
            api_response = api_instance.create_namespaced_custom_object(self.group, self.version, self.namespace,
                                                                        plural=self.plural, body=body, pretty=pretty)
            print(api_response)
            logging.getLogger().info(api_instance)
        except ApiException as e:
            logger.error("Exception when calling CustomObjectsApi->create_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))

    def delete_chaos_object(self, metadata_name):
        print(metadata_name)
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            data = api_instance.delete_namespaced_custom_object(self.group, self.version, self.namespace, self.plural,
                                                                metadata_name)
            logger.info(data)
        except ApiException as e:
            logger.error("Exception when calling CustomObjectsApi->delete_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))

    def list_chaos_object(self):
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            data = api_instance.list_namespaced_custom_object(self.group, self.version, self.namespace,
                                                              plural=self.plural)
            # pprint(data)
        except ApiException as e:
            logger.error("Exception when calling CustomObjectsApi->list_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))
        return data

    def delete_all_chaos_object(self):
        chaos_objects = self.list_chaos_object()
        if len(chaos_objects["items"]) > 0:
            for item in chaos_objects["items"]:
                metadata_name = item["metadata"]["name"]
                self.delete_chaos_object(metadata_name)
