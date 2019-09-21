import logging
import inspect
# from utils import singleton

logger = logging.getLogger(__name__)


class ProviderManager:
    PROVIDERS = {}

    @classmethod
    def register_service_provider(cls, target):
        if inspect.isfunction(target):
            cls.PROVIDERS[target.__name__] = target
        elif inspect.isclass(target):
            name = target.__dict__.get('NAME', None)
            name = name if name else target.__class__.__name__
            cls.PROVIDERS[name] = target
        else:
            assert False, 'Cannot register_service_provider for: {}'.format(target)
        return target

    @classmethod
    def get_provider(cls, name):
        return cls.PROVIDERS.get(name, None)

from sd import kubernetes_provider
