from __future__ import print_function
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from chaos import constants as cf
from utils.util_log import test_log as log


class ChaosOpt(object):
    def __init__(self, kind, group=cf.DEFAULT_GROUP, version=cf.DEFAULT_VERSION, namespace=cf.NAMESPACE):
        self.group = group
        self.version = version
        self.namespace = namespace
        self.plural = kind.lower()

    def create_chaos_object(self, body):
        pretty = 'true'
        config.load_kube_config()
        api_instance = client.CustomObjectsApi()
        try:
            api_response = api_instance.create_namespaced_custom_object(self.group, self.version, self.namespace,
                                                                        plural=self.plural, body=body, pretty=pretty)
            log.debug(f"create chaos response: {api_response}")
        except ApiException as e:
            log.error("Exception when calling CustomObjectsApi->create_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))

    def delete_chaos_object(self, metadata_name, raise_ex=True):
        print(metadata_name)
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            data = api_instance.delete_namespaced_custom_object(self.group, self.version, self.namespace, self.plural,
                                                                metadata_name)
            log.debug(f"delete chaos response: {data}")
        except ApiException as e:
            if raise_ex:
                log.error("Exception when calling CustomObjectsApi->delete_namespaced_custom_object: %s\n" % e)
                raise Exception(str(e))

    def list_chaos_object(self):
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            data = api_instance.list_namespaced_custom_object(self.group, self.version, self.namespace,
                                                              plural=self.plural)
        except ApiException as e:
            log.error("Exception when calling CustomObjectsApi->list_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))
        return data

    def delete_all_chaos_object(self):
        chaos_objects = self.list_chaos_object()
        if len(chaos_objects["items"]) > 0:
            for item in chaos_objects["items"]:
                metadata_name = item["metadata"]["name"]
                self.delete_chaos_object(metadata_name)

