import os
import inspect
from functools import partial
from utils.pluginextension import MiPluginBase as PluginBase


class BaseMixin(object):

    def __init__(self, package_name, searchpath=None):
        self.plugin_package_name = package_name
        caller_path = os.path.dirname(inspect.stack()[1][1])
        get_path = partial(os.path.join, caller_path)
        plugin_base = PluginBase(package=self.plugin_package_name,
                                 searchpath=[get_path('./plugins')])
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
        if not class_name:
            raise RuntimeError('Please specify \'{}\' class_name first!'.format(self.PLUGIN_TYPE))

        plugin_class = self.plugin(class_name.lower())
        if not plugin_class:
            raise RuntimeError('{} Plugin \'{}\' Not Installed!'.format(self.PLUGIN_TYPE, class_name))

        return self._create(plugin_class, **kwargs)
