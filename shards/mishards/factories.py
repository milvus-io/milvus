import time
import datetime
import random
import factory
from factory.alchemy import SQLAlchemyModelFactory
from faker import Faker
from faker.providers import BaseProvider

from milvus.client.types import MetricType
from mishards import db
from mishards.models import Tables, TableFiles


class FakerProvider(BaseProvider):
    def this_date(self):
        t = datetime.datetime.today()
        return (t.year - 1900) * 10000 + (t.month - 1) * 100 + t.day


factory.Faker.add_provider(FakerProvider)


class TablesFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Tables
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    table_id = factory.Faker('uuid4')
    state = factory.Faker('random_element', elements=(0, 1))
    dimension = factory.Faker('random_element', elements=(256, 512))
    created_on = int(time.time())
    index_file_size = 0
    engine_type = factory.Faker('random_element', elements=(0, 1, 2, 3))
    metric_type = factory.Faker('random_element', elements=(MetricType.L2, MetricType.IP))
    nlist = 16384


class TableFilesFactory(SQLAlchemyModelFactory):
    class Meta:
        model = TableFiles
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    table = factory.SubFactory(TablesFactory)
    engine_type = factory.Faker('random_element', elements=(0, 1, 2, 3))
    file_id = factory.Faker('uuid4')
    file_type = factory.Faker('random_element', elements=(0, 1, 2, 3, 4))
    file_size = factory.Faker('random_number')
    updated_time = int(time.time())
    created_on = int(time.time())
    date = factory.Faker('this_date')
