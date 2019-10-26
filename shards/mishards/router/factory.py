import os
import logging
from functools import partial
from utils.pluginextension import MiPluginBase as PluginBase

logger = logging.getLogger(__name__)

here = os.path.abspath(os.path.dirname(__file__))
get_path = partial(os.path.join, here)

PLUGIN_PACKAGE_NAME = 'mishards.router.plugins'
plugin_base = PluginBase(package=PLUGIN_PACKAGE_NAME,
                         searchpath=[get_path('./plugins')])


class RouterFactory(object):
    PLUGIN_TYPE = 'Router'

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

    def create(self, class_name, class_config=None, **kwargs):
        if not class_name:
            raise RuntimeError('Please specify router class_name first!')

        this_class = self.plugin(class_name.lower())
        if not this_class:
            raise RuntimeError('{} Plugin \'{}\' Not Installed!'.format(self.PLUGIN_TYPE, class_name))

        router = this_class.create(class_config, **kwargs)
        return router
