import sys

from pymilvus import (
    AnnSearchRequest,
    AsyncMilvusClient,
    RRFRanker,
)
from pymilvus.orm.collection import CollectionSchema
from pymilvus.orm.types import CONSISTENCY_STRONG

from check.func_check import ResponseChecker
from utils.api_request import api_request, logger_interceptor


class AsyncMilvusClientWrapper:
    async_milvus_client = None

    def __init__(self, active_trace=False):
        self.active_trace = active_trace

    def init_async_client(
        self,
        uri: str = "http://localhost:19530",
        user: str = "",
        password: str = "",
        db_name: str = "",
        token: str = "",
        timeout: float | None = None,
        active_trace=False,
        check_task=None,
        check_items=None,
        **kwargs,
    ):
        self.active_trace = active_trace

        """ In order to distinguish the same name of collection """
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([AsyncMilvusClient, uri, user, password, db_name, token, timeout], **kwargs)
        self.async_milvus_client = res if is_succ else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ, **kwargs).run()
        return res, check_result

    @logger_interceptor()
    async def list_collections(self, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.list_collections(timeout, **kwargs)

    @logger_interceptor()
    async def has_collection(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.has_collection(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def has_partition(self, collection_name: str, partition_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.has_partition(collection_name, partition_name, timeout, **kwargs)

    @logger_interceptor()
    async def describe_collection(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.describe_collection(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def list_partitions(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.list_partitions(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def get_collection_stats(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.get_collection_stats(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def flush(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.flush(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def get_load_state(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.get_load_state(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def describe_index(self, collection_name: str, index_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.describe_index(collection_name, index_name, timeout, **kwargs)

    @logger_interceptor()
    async def create_database(self, db_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.create_database(db_name, timeout, **kwargs)

    @logger_interceptor()
    async def drop_database(self, db_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.drop_database(db_name, timeout, **kwargs)

    @logger_interceptor()
    async def list_databases(self, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.list_databases(timeout, **kwargs)

    @logger_interceptor()
    async def list_indexes(self, collection_name: str, field_name: str = "", **kwargs):
        return await self.async_milvus_client.list_indexes(collection_name, field_name, **kwargs)

    @logger_interceptor()
    async def create_collection(
        self,
        collection_name: str,
        dimension: int | None = None,
        primary_field_name: str = "id",  # default is "id"
        id_type: str = "int",  # or "string",
        vector_field_name: str = "vector",  # default is  "vector"
        metric_type: str = "COSINE",
        auto_id: bool = False,
        timeout: float | None = None,
        schema: CollectionSchema | None = None,
        index_params=None,
        **kwargs,
    ):
        kwargs["consistency_level"] = kwargs.get("consistency_level", CONSISTENCY_STRONG)

        return await self.async_milvus_client.create_collection(
            collection_name,
            dimension,
            primary_field_name,
            id_type,
            vector_field_name,
            metric_type,
            auto_id,
            timeout,
            schema,
            index_params,
            **kwargs,
        )

    @logger_interceptor()
    async def drop_collection(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.drop_collection(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def load_collection(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.load_collection(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def release_collection(self, collection_name, timeout=None, **kwargs):
        return await self.async_milvus_client.release_collection(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def truncate_collection(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.truncate_collection(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def list_persistent_segments(self, collection_name: str, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.list_persistent_segments(collection_name, timeout, **kwargs)

    @logger_interceptor()
    async def create_index(self, collection_name: str, index_params, timeout: float | None = None, **kwargs):
        return await self.async_milvus_client.create_index(collection_name, index_params, timeout, **kwargs)

    @logger_interceptor()
    async def drop_index(self, collection_name, index_name, timeout=None, **kwargs):
        return await self.async_milvus_client.drop_index(collection_name, index_name, timeout, **kwargs)

    # @logger_interceptor()
    # async def list_indexes(self, collection_name, field_name="", timeout=None, **kwargs):
    #     return await self.async_milvus_client.list_indexes(collection_name, field_name, timeout, **kwargs)

    @logger_interceptor()
    async def create_partition(self, collection_name, partition_name, timeout=None, **kwargs):
        return await self.async_milvus_client.create_partition(collection_name, partition_name, timeout, **kwargs)

    @logger_interceptor()
    async def drop_partition(self, collection_name, partition_name, timeout=None, **kwargs):
        return await self.async_milvus_client.drop_partition(collection_name, partition_name, timeout, **kwargs)

    @logger_interceptor()
    async def load_partitions(self, collection_name, partition_names, timeout=None, **kwargs):
        return await self.async_milvus_client.load_partitions(collection_name, partition_names, timeout, **kwargs)

    @logger_interceptor()
    async def release_partitions(self, collection_name, partition_names, timeout=None, **kwargs):
        return await self.async_milvus_client.release_partitions(collection_name, partition_names, timeout, **kwargs)

    @logger_interceptor()
    async def insert(
        self,
        collection_name: str,
        data: dict | list[dict],
        timeout: float | None = None,
        partition_name: str | None = "",
        **kwargs,
    ):
        return await self.async_milvus_client.insert(collection_name, data, timeout, partition_name, **kwargs)

    @logger_interceptor()
    async def upsert(
        self,
        collection_name: str,
        data: dict | list[dict],
        timeout: float | None = None,
        partition_name: str | None = "",
        **kwargs,
    ):
        return await self.async_milvus_client.upsert(collection_name, data, timeout, partition_name, **kwargs)

    @logger_interceptor()
    async def search(
        self,
        collection_name: str,
        data: list[list] | list,
        filter: str = "",
        limit: int = 10,
        output_fields: list[str] | None = None,
        search_params: dict | None = None,
        timeout: float | None = None,
        partition_names: list[str] | None = None,
        anns_field: str | None = None,
        **kwargs,
    ):
        return await self.async_milvus_client.search(
            collection_name,
            data,
            filter,
            limit,
            output_fields,
            search_params,
            timeout,
            partition_names,
            anns_field,
            **kwargs,
        )

    @logger_interceptor()
    async def hybrid_search(
        self,
        collection_name: str,
        reqs: list[AnnSearchRequest],
        ranker: RRFRanker,
        limit: int = 10,
        output_fields: list[str] | None = None,
        timeout: float | None = None,
        partition_names: list[str] | None = None,
        **kwargs,
    ):
        return await self.async_milvus_client.hybrid_search(
            collection_name, reqs, ranker, limit, output_fields, timeout, partition_names, **kwargs
        )

    @logger_interceptor()
    async def query(
        self,
        collection_name: str,
        filter: str = "",
        output_fields: list[str] | None = None,
        timeout: float | None = None,
        ids: list | str | int | None = None,
        partition_names: list[str] | None = None,
        **kwargs,
    ):
        return await self.async_milvus_client.query(
            collection_name, filter, output_fields, timeout, ids, partition_names, **kwargs
        )

    @logger_interceptor()
    async def get(
        self,
        collection_name: str,
        ids: list | str | int,
        output_fields: list[str] | None = None,
        timeout: float | None = None,
        partition_names: list[str] | None = None,
        **kwargs,
    ):
        return await self.async_milvus_client.get(
            collection_name, ids, output_fields, timeout, partition_names, **kwargs
        )

    @logger_interceptor()
    async def delete(
        self,
        collection_name: str,
        ids: list | str | int | None = None,
        timeout: float | None = None,
        filter: str | None = None,
        partition_name: str | None = None,
        **kwargs,
    ):
        return await self.async_milvus_client.delete(collection_name, ids, timeout, filter, partition_name, **kwargs)

    @classmethod
    def create_schema(cls, **kwargs):
        kwargs["check_fields"] = False  # do not check fields for now
        return CollectionSchema([], **kwargs)

    @classmethod
    def prepare_index_params(cls, field_name: str = "", **kwargs):
        res, check = api_request([AsyncMilvusClient.prepare_index_params, field_name], **kwargs)
        return res, check

    @logger_interceptor()
    async def close(self, **kwargs):
        return await self.async_milvus_client.close(**kwargs)
