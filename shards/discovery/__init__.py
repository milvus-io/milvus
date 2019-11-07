import os
import os
import sys
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

import logging
from utils import dotdict

logger = logging.getLogger(__name__)


class DiscoveryConfig(dotdict):
    CONFIG_PREFIX = 'DISCOVERY_'

    def dump(self):
        logger.info('----------- DiscoveryConfig -----------------')
        for k, v in self.items():
            logger.info('{}: {}'.format(k, v))
        if len(self) <= 0:
            logger.error('    Empty DiscoveryConfig Found!            ')
        logger.info('---------------------------------------------')

    @classmethod
    def Create(cls, **kwargs):
        o = cls()

        for k, v in os.environ.items():
            if not k.startswith(cls.CONFIG_PREFIX):
                continue
            o[k] = v
        for k, v in kwargs.items():
            o[k] = v

        o.dump()
        return o
