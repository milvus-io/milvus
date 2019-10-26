import importlib
from pluginbase import PluginBase, PluginSource


class MiPluginSource(PluginSource):
    def load_plugin(self, name):
        if '.' in name:
            raise ImportError('Plugin names cannot contain dots.')
        with self:
            return importlib.import_module(self.base.package + '.' + name)


class MiPluginBase(PluginBase):
    def make_plugin_source(self, *args, **kwargs):
        return MiPluginSource(self, *args, **kwargs)
