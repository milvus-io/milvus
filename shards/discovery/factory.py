import os
import logging
from functools import partial
from utils.pluginextension import MiPluginBase as PluginBase
from discovery import DiscoveryConfig

logger = logging.getLogger(__name__)

here = os.path.abspath(os.path.dirname(__file__))
get_path = partial(os.path.join, here)

PLUGIN_PACKAGE_NAME = 'discovery.plugins'
plugin_base = PluginBase(package=PLUGIN_PACKAGE_NAME,
                         searchpath=[get_path('./plugins')])


class DiscoveryFactory(object):
    PLUGIN_TYPE = 'Discovery'

    def __init__(self, searchpath=None):
        self.plugin_package_name = PLUGIN_PACKAGE_NAME
        self.class_map = {}
        searchpath = searchpath if searchpath else []
        searchpath = [searchpath] if isinstance(searchpath, str) else searchpath
        self.source = plugin_base.make_plugin_source(searchpath=searchpath,
                                                     identifier=self.__class__.__name__)

        for plugin_name in self.source.list_plugins():
            plugin = self.source.load_plugin(plugin_name)
            plugin.setup(self)

    def on_plugin_setup(self, plugin_class):
        name = getattr(plugin_class, 'name', plugin_class.__name__)
        self.class_map[name.lower()] = plugin_class

    def plugin(self, name):
        return self.class_map.get(name, None)

    def create(self, class_name, **kwargs):
        conn_mgr = kwargs.pop('conn_mgr', None)
        if not conn_mgr:
            raise RuntimeError('Please pass conn_mgr to create discovery!')

        if not class_name:
            raise RuntimeError('Please specify \'{}\' class_name first!'.format(self.PLUGIN_TYPE))

        plugin_class = self.plugin(class_name.lower())
        if not plugin_class:
            raise RuntimeError('{} Plugin \'{}\' Not Installed!'.format(self.PLUGIN_TYPE, class_name))

        plugin_config = DiscoveryConfig.Create()
        plugin = plugin_class.create(plugin_config=plugin_config, conn_mgr=conn_mgr, **kwargs)
        return plugin
