import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import settings
from mishards import  connect_mgr, grpc_server as server

def main():
    connect_mgr.register('WOSERVER', settings.WOSERVER)
    connect_mgr.register('TEST', 'tcp://127.0.0.1:19530')
    server.run(port=settings.SERVER_PORT)
    return 0

if __name__ == '__main__':
    sys.exit(main())
