import logging
from discovery import DiscoveryConfig
from utils.plugins import BaseMixin

logger = logging.getLogger(__name__)
PLUGIN_PACKAGE_NAME = 'discovery.plugins'


class DiscoveryFactory(BaseMixin):
    PLUGIN_TYPE = 'Discovery'
    def __init__(self, searchpath=None):
        super().__init__(searchpath=searchpath, package_name=PLUGIN_PACKAGE_NAME)

    def _create(self, plugin_class, **kwargs):
        conn_mgr = kwargs.pop('conn_mgr', None)
        if not conn_mgr:
            raise RuntimeError('Please pass conn_mgr to create discovery!')

        plugin_config = DiscoveryConfig.Create()
        plugin = plugin_class.create(plugin_config=plugin_config, conn_mgr=conn_mgr, **kwargs)
        return plugin
