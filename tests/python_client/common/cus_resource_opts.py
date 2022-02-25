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
        if kind.lower()[-1] != "s":
            self.plural = kind.lower() + "s"
        else:
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
        return api_response

    def delete(self, metadata_name, raise_ex=True):
        """delete or uninstall a custom resource in k8s"""
        print(metadata_name)
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            api_response = api_instance.delete_namespaced_custom_object(self.group, self.version, self.namespace,
                                                                        self.plural,
                                                                        metadata_name)
            log.debug(f"delete custom resource response: {api_response}")
        except ApiException as e:
            if raise_ex:
                log.error("Exception when calling CustomObjectsApi->delete_namespaced_custom_object: %s\n" % e)
                raise Exception(str(e))

    def patch(self, metadata_name, body):
        """patch a custom resource in k8s"""
        config.load_kube_config()
        api_instance = client.CustomObjectsApi()
        try:
            api_response = api_instance.patch_namespaced_custom_object(self.group, self.version, self.namespace,
                                                                       plural=self.plural,
                                                                       name=metadata_name,
                                                                       body=body)
            log.debug(f"patch custom resource response: {api_response}")
        except ApiException as e:
            log.error("Exception when calling CustomObjectsApi->patch_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))
        return api_response

    def list_all(self):
        """list all the customer resources in k8s"""
        pretty = 'true'
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            api_response = api_instance.list_namespaced_custom_object(self.group, self.version, self.namespace,
                                                                      plural=self.plural, pretty=pretty)
            log.debug(f"list custom resource response: {api_response}")
        except ApiException as e:
            log.error("Exception when calling CustomObjectsApi->list_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))
        return api_response

    def get(self, metadata_name):
        """get a customer resources by name in k8s"""
        try:
            config.load_kube_config()
            api_instance = client.CustomObjectsApi()
            api_response = api_instance.get_namespaced_custom_object(self.group, self.version,
                                                                     self.namespace, self.plural,
                                                                     name=metadata_name)
            # log.debug(f"get custom resource response: {api_response}")
        except ApiException as e:
            log.error("Exception when calling CustomObjectsApi->get_namespaced_custom_object: %s\n" % e)
            raise Exception(str(e))
        return api_response

    def delete_all(self):
        """delete all the customer resources in k8s"""
        cus_objects = self.list_all()
        if len(cus_objects["items"]) > 0:
            for item in cus_objects["items"]:
                metadata_name = item["metadata"]["name"]
                self.delete(metadata_name)
