import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mishards import (
        settings, create_app)

def main():
    server = create_app()
    server.run(port=settings.SERVER_PORT)
    return 0

if __name__ == '__main__':
    sys.exit(main())
