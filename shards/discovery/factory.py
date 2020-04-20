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
        readonly_topo = kwargs.pop('readonly_topo', None)
        if not readonly_topo:
            raise RuntimeError('Please pass readonly_topo to create discovery!')

        plugin_config = DiscoveryConfig.Create()
        plugin = plugin_class.Create(plugin_config=plugin_config, readonly_topo=readonly_topo, **kwargs)
        return plugin
