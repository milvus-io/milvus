import importlib.util
from pluginbase import PluginBase, PluginSource


class MiPluginSource(PluginSource):
    def load_plugin(self, name):
        plugin = super().load_plugin(name)
        spec = importlib.util.spec_from_file_location(self.base.package + '.' + name, plugin.__file__)
        plugin = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(plugin)
        return plugin


class MiPluginBase(PluginBase):
    def make_plugin_source(self, *args, **kwargs):
        return MiPluginSource(self, *args, **kwargs)
