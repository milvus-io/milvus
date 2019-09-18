import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mishards import (
        settings,
        db, connect_mgr,
        discover,
        grpc_server as server)

def main():
    discover.start()
    connect_mgr.register('WOSERVER', settings.WOSERVER if not settings.TESTING else settings.TESTING_WOSERVER)
    server.run(port=settings.SERVER_PORT)
    return 0

if __name__ == '__main__':
    sys.exit(main())
