import os
import logging
from tracer import Tracer
from utils.plugins import BaseMixin

logger = logging.getLogger(__name__)
PLUGIN_PACKAGE_NAME = 'tracer.plugins'


class TracerFactory(BaseMixin):
    PLUGIN_TYPE = 'Tracer'

    def __init__(self, searchpath=None):
        super().__init__(searchpath=searchpath, package_name=PLUGIN_PACKAGE_NAME)

    def create(self, class_name, **kwargs):
        if not class_name:
            return Tracer()
        return super().create(class_name, **kwargs)

    def _create(self, plugin_class, **kwargs):
        plugin_config = kwargs.pop('plugin_config', None)
        if not plugin_config:
            raise RuntimeError('\'{}\' Plugin Config is Required!'.format(self.PLUGIN_TYPE))

        plugin = plugin_class.Create(plugin_config=plugin_config, **kwargs)
        return plugin
