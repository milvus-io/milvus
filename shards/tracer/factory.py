import os
import logging
from functools import partial
from pluginbase import PluginBase
from tracer import Tracer


logger = logging.getLogger(__name__)

here = os.path.abspath(os.path.dirname(__file__))
get_path = partial(os.path.join, here)

PLUGIN_PACKAGE_NAME = 'tracer.plugins'
plugin_base = PluginBase(package=PLUGIN_PACKAGE_NAME,
                         searchpath=[get_path('./plugins')])


class TracerFactory(object):
    def __init__(self, searchpath=None):
        self.plugin_package_name = PLUGIN_PACKAGE_NAME
        self.tracer_map = {}
        searchpath = searchpath if searchpath else []
        searchpath = [searchpath] if isinstance(searchpath, str) else searchpath
        self.source = plugin_base.make_plugin_source(searchpath=searchpath,
                                                     identifier=self.__class__.__name__)

        for plugin_name in self.source.list_plugins():
            plugin = self.source.load_plugin(plugin_name)
            plugin.setup(self)

    def on_plugin_setup(self, plugin_class):
        name = getattr(plugin_class, 'name', plugin_class.__name__)
        self.tracer_map[name.lower()] = plugin_class

    def plugin(self, name):
        return self.tracer_map.get(name, None)

    def create(self,
               tracer_type,
               tracer_config,
               span_decorator=None,
               **kwargs):
        if not tracer_type:
            return Tracer()
        plugin_class = self.plugin(tracer_type.lower())
        if not plugin_class:
            raise RuntimeError('Tracer Plugin \'{}\' not installed!'.format(tracer_type))

        tracer = plugin_class.create(tracer_config, span_decorator=span_decorator, **kwargs)
        return tracer
