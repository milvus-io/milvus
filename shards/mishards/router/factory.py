import os
import logging
from utils.plugins import BaseMixin

logger = logging.getLogger(__name__)
PLUGIN_PACKAGE_NAME = 'mishards.router.plugins'


class RouterFactory(BaseMixin):
    PLUGIN_TYPE = 'Router'

    def __init__(self, searchpath=None):
        super().__init__(searchpath=searchpath, package_name=PLUGIN_PACKAGE_NAME)

    def _create(self, plugin_class, **kwargs):
        router = plugin_class.Create(**kwargs)
        return router
