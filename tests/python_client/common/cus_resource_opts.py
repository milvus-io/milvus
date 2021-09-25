from __future__ import print_function
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from utils.util_log import test_log as log

_GROUP = 'milvus.io'
_VERSION = 'v1alpha1'
_NAMESPACE = "default"


class CustomResourceOperations(object):
    def __init__(self, kind, group=_GROUP, version=_VERSION, namespace=_NAMESPACE):
        self.group = group
        self.version = version
        self.namespace = namespace
        self.plural = kind.lower()

    def create(self, body):
        """create or apply a custom resource in k8s"""
        pretty = 'true'
        config.load_kube_config()
        api_instance = client.CustomObjectsApi()
        try:
            api_response = api_instance.create_namespaced_custom_object(self.group, self.version, self.namespace,
                                                                        plural=self.plural, body=body, pretty=pretty)
            log.debug(f"create custom resource response: {api_response}")
        except ApiException as e:
            log.error("Exception when calling CustomObjectsApi->create_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))

    def delete(self, metadata_name, raise_ex=True):
        """delete or uninstall a custom resource in k8s"""
        print(metadata_name)
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            data = api_instance.delete_namespaced_custom_object(self.group, self.version, self.namespace, self.plural,
                                                                metadata_name)
            log.debug(f"delete custom resource response: {data}")
        except ApiException as e:
            if raise_ex:
                log.error("Exception when calling CustomObjectsApi->delete_namespaced_custom_object: %s\n" % e)
                raise Exception(str(e))

    def list_all(self):
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            data = api_instance.list_namespaced_custom_object(self.group, self.version, self.namespace,
                                                              plural=self.plural)
        except ApiException as e:
            log.error("Exception when calling CustomObjectsApi->list_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))
        return data

    def delete_all(self):
        cus_objects = self.list_all()
        if len(cus_objects["items"]) > 0:
            for item in cus_objects["items"]:
                metadata_name = item["metadata"]["name"]
                self.delete(metadata_name)

