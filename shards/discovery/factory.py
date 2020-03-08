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
        topo = kwargs.pop('topo', None)
        if not topo:
            raise RuntimeError('Please pass topo to create discovery!')

        plugin_config = DiscoveryConfig.Create()
        plugin = plugin_class.Create(plugin_config=plugin_config, topo=topo, **kwargs)
        return plugin
