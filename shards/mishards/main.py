import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mishards import (settings, create_app)


def main():
    server = create_app(settings.DefaultConfig)
    server.run(port=settings.SERVER_PORT)
    return 0


if __name__ == '__main__':
    sys.exit(main())
