import time
from benedict import benedict
from utils.util_log import test_log as log
from common.cus_resource_opts import CustomResourceOperations as CusResource


template_yaml = 'template/default.yaml'
MILVUS_GRP = 'milvus.io'
MILVUS_VER = 'v1alpha1'
MILVUS_PLURAL = 'milvusclusters'
MILVUS_KIND = 'MilvusCluster'


class MilvusOperator(object):
    def __init__(self):
        self.group = MILVUS_GRP
        self.version = MILVUS_VER
        self.plural = MILVUS_PLURAL.lower()


    @staticmethod
    def _update_configs(configs, template=None):
        if not isinstance(configs, dict):
            log.error("customize configurations must be in dict type")
            return None

        if template is None:
            d_configs = benedict()
            d_configs['apiVersion'] = f'{MILVUS_GRP}/{MILVUS_VER}'
            d_configs['kind'] = MILVUS_KIND
        else:
            d_configs = benedict.from_yaml(template)

        for key in configs.keys():
            d_configs[key] = configs[key]

        # return a python dict if it is not none
        return d_configs._dict if d_configs._dict is not None else d_configs

    def install(self, configs, template=None):
        new_configs = self._update_configs(configs, template)
        print(new_configs)
        namespace = new_configs['metadata'].get('namespace', 'default')
        # apply custom resource object to deploy milvus
        cus_res = CusResource(kind=self.plural, group=self.group,
                              version=self.version, namespace=namespace)
        log.info(f"install milvus with configs: {new_configs}")
        return cus_res.create(new_configs)

    def uninstall(self, release_name, namespace='default', delete_depends=True, delete_pvc=True):
        # delete custom resource object to uninstall milvus
        cus_res = CusResource(kind=self.plural, group=self.group,
                              version=self.version, namespace=namespace)
        del_configs = {}
        if delete_depends:
            del_configs = {'spec.dependencies.etcd.inCluster.deletionPolicy': 'Delete',
                           'spec.dependencies.pulsar.inCluster.deletionPolicy': 'Delete',
                           'spec.dependencies.storage.inCluster.deletionPolicy': 'Delete'
                           }
        if delete_pvc:
            del_configs.update({'spec.dependencies.etcd.inCluster.pvcDeletion': True,
                                'spec.dependencies.pulsar.inCluster.pvcDeletion': True,
                                'spec.dependencies.storage.inCluster.pvcDeletion': True
                                })
        if delete_depends or delete_pvc:
            self.upgrade(release_name, del_configs, namespace=namespace)
        cus_res.delete(release_name)

    def upgrade(self, release_name, configs, namespace='default'):
        if not isinstance(configs, dict):
            log.error("customize configurations must be in dict type")
            return None

        d_configs = benedict()

        for key in configs.keys():
            d_configs[key] = configs[key]

        cus_res = CusResource(kind=self.plural, group=self.group,
                              version=self.version, namespace=namespace)
        log.debug(f"upgrade milvus with configs: {d_configs}")
        cus_res.patch(release_name, d_configs)

    def wait_for_healthy(self, release_name, namespace='default', timeout=600):

        cus_res = CusResource(kind=self.plural, group=self.group,
                              version=self.version, namespace=namespace)
        starttime = time.time()
        log.info(f"start to check healthy: {starttime}")
        while time.time() < starttime + timeout:
            time.sleep(10)
            res_object = cus_res.get(release_name)
            if res_object.get('status', None) is not None:
                if 'Healthy' == res_object['status']['status']:
                    log.info(f"milvus healthy in {time.time()-starttime} seconds")
                    return True
        log.info(f"end to check healthy until timeout {timeout}")
        return False

    def endpoint(self, release_name, namespace='default'):
        endpoint = None
        cus_res = CusResource(kind=self.plural, group=self.group,
                              version=self.version, namespace=namespace)
        res_object = cus_res.get(release_name)
        if res_object.get('status', None) is not None:
            endpoint = res_object['status']['endpoint']

        return endpoint


if __name__ == '__main__':

    namespace = 'chaos-testing'
    name = 'milvus-513b'
    cus_configs = {'spec.components.image': 'milvusdb/milvus-dev:master-20211020-b40513b',
                   'metadata.namespace': namespace,
                   'metadata.name': name,
                   'apiVersion': 'milvus.io/v1alpha1',
                   'kind': 'MilvusCluster',
                   'spec.components.queryNode.replicas': 2,
                   'spec.components.queryNode.resources.limits.memory': '2048Mi'
                   }

    milvusOp = MilvusOperator()
    milvus_instance = milvusOp.install(cus_configs, template=None)
    result = milvusOp.wait_for_healthy(name, namespace=namespace)
    endpoint = milvusOp.endpoint(name, namespace=namespace)
    print(endpoint)
    log.info(f"install milvus healthy: {result}")

    # n_configs = {'spec.components.queryNode.replicas': 1,
    #              'spec.components.proxy.serviceType': 'LoadBalancer'
    #              }
    # milvusOp.upgrade(name, n_configs, namespace=namespace)
    # result = milvusOp.wait_for_healthy(name, namespace=namespace)
    # log.info(f"upgrade milvus healthy: {result}")
    # endpoint = milvusOp.endpoint(name, namespace=namespace)
    # print(endpoint)

    milvusOp.uninstall(name, namespace=namespace)

