import logging
import pytest
from mishards import settings, db, create_app

logger = logging.getLogger(__name__)

def clear_data(session):
    meta = db.metadata
    for table in reversed(meta.sorted_tables):
        session.execute(table.delete())
    session.commit()

# @pytest.fixture(scope="module")
@pytest.fixture
def app(request):
    app = create_app(settings.TestingConfig)
    db.drop_all()
    db.create_all()

    yield app

    db.drop_all()
