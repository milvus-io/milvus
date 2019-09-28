import logging
import pytest
from mishards import settings, db, create_app

logger = logging.getLogger(__name__)

@pytest.fixture
def app(request):
    app = create_app(settings.TestingConfig)
    db.drop_all()
    db.create_all()

    yield app

    db.drop_all()
