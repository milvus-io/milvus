import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from urllib.parse import urlparse
import socket

from mishards import (
        settings,
        db, connect_mgr,
        discover,
        grpc_server as server)

def main():
    discover.start()
    woserver = settings.WOSERVER if not settings.TESTING else settings.TESTING_WOSERVER
    url = urlparse(woserver)
    connect_mgr.register('WOSERVER',
            '{}://{}:{}'.format(url.scheme, socket.gethostbyname(url.hostname), url.port))
    server.run(port=settings.SERVER_PORT)
    return 0

if __name__ == '__main__':
    sys.exit(main())
