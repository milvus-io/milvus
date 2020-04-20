import os
import logging
import pytest
import grpc
import mock
import tempfile
import shutil
import time
from mishards import settings, db, create_app

logger = logging.getLogger(__name__)

tpath = tempfile.mkdtemp()
dirpath = '{}/db'.format(tpath)
filepath = '{}/meta.sqlite'.format(dirpath)
os.makedirs(dirpath, 0o777)
settings.TestingConfig.SQLALCHEMY_DATABASE_URI = 'sqlite:///{}?check_same_thread=False'.format(
    filepath)


@pytest.fixture
def app(request):
    from mishards.connections import ConnectionGroup
    ConnectionGroup.on_pre_add = mock.MagicMock(return_value=(True,))
    time.sleep(0.1)
    app = create_app(settings.TestingConfig)
    db.drop_all()
    db.create_all()

    yield app

    db.drop_all()
    app.stop()
    # shutil.rmtree(tpath)


@pytest.fixture
def started_app(app):
    app.on_pre_run()
    app.start(settings.SERVER_TEST_PORT)

    yield app

    app.stop()
