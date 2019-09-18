import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import settings
from mishards import  (connect_mgr,
        discover,
        grpc_server as server)

def main():
    try:
        discover.start()
        connect_mgr.register('WOSERVER', settings.WOSERVER if not settings.TESTING else settings.TESTING_WOSERVER)
        server.run(port=settings.SERVER_PORT)
        return 0
    except Exception as e:
        logger.error(e)
        return 1

if __name__ == '__main__':
    sys.exit(main())
