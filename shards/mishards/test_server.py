import logging
import pytest
import mock
import datetime
import random
import faker
import inspect
from milvus import Milvus
from milvus.client.types import Status, IndexType, MetricType
from milvus.client.abstract import IndexParam, CollectionSchema
from milvus.grpc_gen import status_pb2, milvus_pb2
from mishards import db, create_app, settings
from mishards.service_handler import ServiceHandler
from mishards.grpc_utils.grpc_args_parser import GrpcArgsParser as Parser
from mishards.factories import TableFilesFactory, TablesFactory, TableFiles, Tables
from mishards.router import RouterMixin
from mishards.connections import (Connection,
        ConnectionPool, ConnectionTopology, ConnectionGroup)

logger = logging.getLogger(__name__)

OK = Status(code=Status.SUCCESS, message='Success')
BAD = Status(code=Status.PERMISSION_DENIED, message='Fail')


@pytest.mark.usefixtures('started_app')
class TestServer:

    @property
    def client(self):
        m = Milvus()
        m.connect(host='localhost', port=settings.SERVER_TEST_PORT)
        return m

    def test_cmd(self, started_app):
        ServiceHandler._get_server_version = mock.MagicMock(return_value=(OK,
                                                                          ''))
        status, _ = self.client.server_version()
        assert status.OK()

        Parser.parse_proto_Command = mock.MagicMock(return_value=(BAD, 'cmd'))
        status, _ = self.client.server_version()
        assert not status.OK()

    def test_drop_index(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name
        ServiceHandler._drop_index = mock.MagicMock(return_value=OK)
        status = self.client.drop_index(collection_name)
        assert status.OK()

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(BAD, collection_name))
        status = self.client.drop_index(collection_name)
        assert not status.OK()

    def test_describe_index(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name
        index_type = IndexType.FLAT
        params = {'nlist': 1}
        index_param = IndexParam(collection_name=collection_name,
                                 index_type=index_type,
                                 params=params)
        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(OK, collection_name))
        ServiceHandler._describe_index = mock.MagicMock(
            return_value=(OK, index_param))
        status, ret = self.client.describe_index(collection_name)
        assert status.OK()
        assert ret._collection_name == index_param._collection_name

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(BAD, collection_name))
        status, _ = self.client.describe_index(collection_name)
        assert not status.OK()

    def test_preload(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(OK, collection_name))
        ServiceHandler._preload_collection = mock.MagicMock(return_value=OK)
        status = self.client.preload_collection(collection_name)
        assert status.OK()

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(BAD, collection_name))
        status = self.client.preload_collection(collection_name)
        assert not status.OK()

    @pytest.mark.skip
    def test_delete_by_range(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name

        unpacked = collection_name, datetime.datetime.today(
        ), datetime.datetime.today()

        Parser.parse_proto_DeleteByRangeParam = mock.MagicMock(
            return_value=(OK, unpacked))
        ServiceHandler._delete_by_range = mock.MagicMock(return_value=OK)
        status = self.client.delete_vectors_by_range(
            *unpacked)
        assert status.OK()

        Parser.parse_proto_DeleteByRangeParam = mock.MagicMock(
            return_value=(BAD, unpacked))
        status = self.client.delete_vectors_by_range(
            *unpacked)
        assert not status.OK()

    def test_count_collection(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name
        count = random.randint(100, 200)

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(OK, collection_name))
        ServiceHandler._count_collection = mock.MagicMock(return_value=(OK, count))
        status, ret = self.client.count_collection(collection_name)
        assert status.OK()
        assert ret == count

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(BAD, collection_name))
        status, _ = self.client.count_collection(collection_name)
        assert not status.OK()

    def test_show_collections(self, started_app):
        collections = ['t1', 't2']
        ServiceHandler._show_collections = mock.MagicMock(return_value=(OK, collections))
        status, ret = self.client.show_collections()
        assert status.OK()
        assert ret == collections

    def test_describe_collection(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name
        dimension = 128
        nlist = 1
        collection_schema = CollectionSchema(collection_name=collection_name,
                                   index_file_size=100,
                                   metric_type=MetricType.L2,
                                   dimension=dimension)
        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(OK, collection_schema.collection_name))
        ServiceHandler._describe_collection = mock.MagicMock(
            return_value=(OK, collection_schema))
        status, _ = self.client.describe_collection(collection_name)
        assert status.OK()

        ServiceHandler._describe_collection = mock.MagicMock(
            return_value=(BAD, collection_schema))
        status, _ = self.client.describe_collection(collection_name)
        assert not status.OK()

        Parser.parse_proto_CollectionName = mock.MagicMock(return_value=(BAD,
                                                                    'cmd'))
        status, ret = self.client.describe_collection(collection_name)
        assert not status.OK()

    def test_insert(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name
        vectors = [[random.random() for _ in range(16)] for _ in range(10)]
        ids = [random.randint(1000000, 20000000) for _ in range(10)]
        ServiceHandler._add_vectors = mock.MagicMock(return_value=(OK, ids))
        status, ret = self.client.add_vectors(
            collection_name=collection_name, records=vectors)
        assert status.OK()
        assert ids == ret

    def test_create_index(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name
        unpacks = collection_name, None
        Parser.parse_proto_IndexParam = mock.MagicMock(return_value=(OK,
                                                                     unpacks))
        ServiceHandler._create_index = mock.MagicMock(return_value=OK)
        status = self.client.create_index(collection_name=collection_name)
        assert status.OK()

        Parser.parse_proto_IndexParam = mock.MagicMock(return_value=(BAD,
                                                                     None))
        status = self.client.create_index(collection_name=collection_name)
        assert not status.OK()

    def test_drop_collection(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(OK, collection_name))
        ServiceHandler._drop_collection = mock.MagicMock(return_value=OK)
        status = self.client.drop_collection(collection_name=collection_name)
        assert status.OK()

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(BAD, collection_name))
        status = self.client.drop_collection(collection_name=collection_name)
        assert not status.OK()

    def test_has_collection(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(OK, collection_name))
        ServiceHandler._has_collection = mock.MagicMock(return_value=(OK, True))
        has = self.client.has_collection(collection_name=collection_name)
        assert has

        Parser.parse_proto_CollectionName = mock.MagicMock(
            return_value=(BAD, collection_name))
        status, has = self.client.has_collection(collection_name=collection_name)
        assert not status.OK()
        assert not has

    def test_create_collection(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name
        dimension = 128
        collection_schema = dict(collection_name=collection_name,
                            index_file_size=100,
                            metric_type=MetricType.L2,
                            dimension=dimension)

        ServiceHandler._create_collection = mock.MagicMock(return_value=OK)
        status = self.client.create_collection(collection_schema)
        assert status.OK()

        Parser.parse_proto_CollectionSchema = mock.MagicMock(return_value=(BAD,
                                                                      None))
        status = self.client.create_collection(collection_schema)
        assert not status.OK()

    def random_data(self, n, dimension):
        return [[random.random() for _ in range(dimension)] for _ in range(n)]

    @pytest.mark.skip
    def test_search(self, started_app):
        collection_name = inspect.currentframe().f_code.co_name
        to_index_cnt = random.randint(10, 20)
        collection = TablesFactory(collection_id=collection_name, state=Tables.NORMAL)
        to_index_files = TableFilesFactory.create_batch(
            to_index_cnt, collection=collection, file_type=TableFiles.FILE_TYPE_TO_INDEX)
        topk = random.randint(5, 10)
        nq = random.randint(5, 10)
        param = {
            'collection_name': collection_name,
            'query_records': self.random_data(nq, collection.dimension),
            'top_k': topk,
            'params': {'nprobe': 2049}
        }

        result = [
            milvus_pb2.TopKQueryResult(query_result_arrays=[
                milvus_pb2.QueryResult(id=i, distance=random.random())
                for i in range(topk)
            ]) for i in range(nq)
        ]

        mock_results = milvus_pb2.TopKQueryResultList(status=status_pb2.Status(
            error_code=status_pb2.SUCCESS, reason="Success"),
            topk_query_result=result)

        collection_schema = CollectionSchema(collection_name=collection_name,
                                   index_file_size=collection.index_file_size,
                                   metric_type=collection.metric_type,
                                   dimension=collection.dimension)

        status, _ = self.client.search_vectors(**param)
        assert status.code == Status.ILLEGAL_ARGUMENT

        param['params']['nprobe'] = 2048
        RouterMixin.connection = mock.MagicMock(return_value=Milvus())
        RouterMixin.query_conn.conn = mock.MagicMock(return_value=Milvus())
        Milvus.describe_collection = mock.MagicMock(return_value=(BAD,
                                                             collection_schema))
        status, ret = self.client.search_vectors(**param)
        assert status.code == Status.COLLECTION_NOT_EXISTS

        Milvus.describe_collection = mock.MagicMock(return_value=(OK, collection_schema))
        Milvus.search_vectors_in_files = mock.MagicMock(
            return_value=mock_results)

        status, ret = self.client.search_vectors(**param)
        assert status.OK()
        assert len(ret) == nq
