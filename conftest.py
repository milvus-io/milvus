import logging
import pytest
import grpc
from mishards import settings, db, create_app

logger = logging.getLogger(__name__)


@pytest.fixture
def app(request):
    app = create_app(settings.TestingConfig)
    db.drop_all()
    db.create_all()

    yield app

    db.drop_all()


@pytest.fixture
def started_app(app):
    app.on_pre_run()
    app.start(app.port)

    yield app

    app.stop()
