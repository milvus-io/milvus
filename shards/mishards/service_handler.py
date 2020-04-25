import logging
import time
import json
import ujson

import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from milvus.grpc_gen import milvus_pb2, milvus_pb2_grpc, status_pb2
from milvus.client import types as Types
from milvus import MetricType

from mishards import (db, exceptions)
from mishards.grpc_utils import mark_grpc_method
from mishards.grpc_utils.grpc_args_parser import GrpcArgsParser as Parser

logger = logging.getLogger(__name__)


class ServiceHandler(milvus_pb2_grpc.MilvusServiceServicer):
    MAX_NPROBE = 2048
    MAX_TOPK = 2048

    def __init__(self, tracer, router, max_workers=multiprocessing.cpu_count(), **kwargs):
        self.collection_meta = {}
        self.error_handlers = {}
        self.tracer = tracer
        self.router = router
        self.max_workers = max_workers

    def _reduce(self, source_ids, ids, source_diss, diss, k, reverse):
        if source_diss[k - 1] <= diss[0]:
            return source_ids, source_diss
        if diss[k - 1] <= source_diss[0]:
            return ids, diss

        source_diss.extend(diss)
        diss_t = enumerate(source_diss)
        diss_m_rst = sorted(diss_t, key=lambda x: x[1])[:k]
        diss_m_out = [id_ for _, id_ in diss_m_rst]

        source_ids.extend(ids)
        id_m_out = [source_ids[i] for i, _ in diss_m_rst]

        return id_m_out, diss_m_out

    def _do_merge(self, files_n_topk_results, topk, reverse=False, **kwargs):
        status = status_pb2.Status(error_code=status_pb2.SUCCESS,
                                   reason="Success")
        if not files_n_topk_results:
            return status, [], []

        merge_id_results = []
        merge_dis_results = []

        calc_time = time.time()
        for files_collection in files_n_topk_results:
            if isinstance(files_collection, tuple):
                status, _ = files_collection
                return status, [], []

            if files_collection.status.error_code != 0:
                return files_collection.status, [], []

            row_num = files_collection.row_num
            # row_num is equal to 0, result is empty
            if not row_num:
                continue

            ids = files_collection.ids
            diss = files_collection.distances  # distance collections
            # TODO: batch_len is equal to topk, may need to compare with topk
            batch_len = len(ids) // row_num

            for row_index in range(row_num):
                id_batch = ids[row_index * batch_len: (row_index + 1) * batch_len]
                dis_batch = diss[row_index * batch_len: (row_index + 1) * batch_len]

                if len(merge_id_results) < row_index:
                    raise ValueError("merge error")
                elif len(merge_id_results) == row_index:
                    # TODO: may bug here
                    merge_id_results.append(id_batch)
                    merge_dis_results.append(dis_batch)
                else:
                    merge_id_results[row_index], merge_dis_results[row_index] = \
                        self._reduce(merge_id_results[row_index], id_batch,
                                     merge_dis_results[row_index], dis_batch,
                                     batch_len,
                                     reverse)

        calc_time = time.time() - calc_time
        logger.info('Merge takes {}'.format(calc_time))

        id_mrege_list = []
        dis_mrege_list = []

        for id_results, dis_results in zip(merge_id_results, merge_dis_results):
            id_mrege_list.extend(id_results)
            dis_mrege_list.extend(dis_results)

        return status, id_mrege_list, dis_mrege_list

    def _do_query(self,
                  context,
                  collection_id,
                  collection_meta,
                  vectors,
                  topk,
                  search_params,
                  partition_tags=None,
                  **kwargs):
        metadata = kwargs.get('metadata', None)

        routing = {}
        p_span = None if self.tracer.empty else context.get_active_span(
        ).context
        with self.tracer.start_span('get_routing', child_of=p_span):
            routing = self.router.routing(collection_id,
                                          partition_tags=partition_tags,
                                          metadata=metadata)
        logger.info('Routing: {}'.format(routing))

        metadata = kwargs.get('metadata', None)

        rs = []
        all_topk_results = []

        def search(addr, collection_id, file_ids, vectors, topk, params, **kwargs):
            logger.info(
                'Send Search Request: addr={};collection_id={};ids={};nq={};topk={};params={}'
                    .format(addr, collection_id, file_ids, len(vectors), topk, params))

            conn = self.router.query_conn(addr, metadata=metadata)
            start = time.time()
            span = kwargs.get('span', None)
            span = span if span else (None if self.tracer.empty else
                                      context.get_active_span().context)

            with self.tracer.start_span('search_{}'.format(addr),
                                        child_of=span):
                ret = conn.conn.search_vectors_in_files(collection_name=collection_id,
                                                        file_ids=file_ids,
                                                        query_records=vectors,
                                                        top_k=topk,
                                                        params=params)
                if ret.status.error_code != 0:
                    logger.error("Search fail {}".format(ret.status))

                end = time.time()
                all_topk_results.append(ret)

        with self.tracer.start_span('do_search', child_of=p_span) as span:
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                for addr, file_ids in routing.items():
                    res = pool.submit(search,
                                      addr,
                                      collection_id,
                                      file_ids,
                                      vectors,
                                      topk,
                                      search_params,
                                      span=span)
                    rs.append(res)

                for res in rs:
                    res.result()

        reverse = collection_meta.metric_type == Types.MetricType.IP
        with self.tracer.start_span('do_merge', child_of=p_span):
            return self._do_merge(all_topk_results,
                                  topk,
                                  reverse=reverse,
                                  metadata=metadata)

    def _create_collection(self, collection_schema):
        return self.router.connection().create_collection(collection_schema)

    @mark_grpc_method
    def CreateCollection(self, request, context):
        _status, unpacks = Parser.parse_proto_CollectionSchema(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        _status, _collection_schema = unpacks
        # if _status.error_code != 0:
        #     logging.warning('[CreateCollection] collection schema error occurred: {}'.format(_status))
        #     return _status

        logger.info('CreateCollection {}'.format(_collection_schema['collection_name']))

        _status = self._create_collection(_collection_schema)

        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    def _has_collection(self, collection_name, metadata=None):
        return self.router.connection(metadata=metadata).has_collection(collection_name)

    @mark_grpc_method
    def HasCollection(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            return milvus_pb2.BoolReply(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message),
                bool_reply=False)

        logger.info('HasCollection {}'.format(_collection_name))

        _status, _bool = self._has_collection(_collection_name,
                                         metadata={'resp_class': milvus_pb2.BoolReply})

        return milvus_pb2.BoolReply(status=status_pb2.Status(
            error_code=_status.code, reason=_status.message),
            bool_reply=_bool)

    @mark_grpc_method
    def CreatePartition(self, request, context):
        _collection_name, _tag = Parser.parse_proto_PartitionParam(request)
        _status = self.router.connection().create_partition(_collection_name, _tag)
        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    @mark_grpc_method
    def DropPartition(self, request, context):
        _collection_name, _tag = Parser.parse_proto_PartitionParam(request)

        _status = self.router.connection().drop_partition(_collection_name, _tag)
        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    @mark_grpc_method
    def ShowPartitions(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)
        if not _status.OK():
            return milvus_pb2.PartitionList(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message),
                partition_array=[])

        logger.info('ShowPartitions {}'.format(_collection_name))

        _status, partition_array = self.router.connection().show_partitions(_collection_name)

        return milvus_pb2.PartitionList(status=status_pb2.Status(
            error_code=_status.code, reason=_status.message),
            partition_tag_array=[param.tag for param in partition_array])

    def _drop_collection(self, collection_name):
        return self.router.connection().drop_collection(collection_name)

    @mark_grpc_method
    def DropCollection(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        logger.info('DropCollection {}'.format(_collection_name))

        _status = self._drop_collection(_collection_name)

        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    def _create_index(self, collection_name, index_type, param):
        return self.router.connection().create_index(collection_name, index_type, param)

    @mark_grpc_method
    def CreateIndex(self, request, context):
        _status, unpacks = Parser.parse_proto_IndexParam(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        _collection_name, _index_type, _index_param = unpacks

        logger.info('CreateIndex {}'.format(_collection_name))

        # TODO: interface create_collection incompleted
        _status = self._create_index(_collection_name, _index_type, _index_param)

        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    def _add_vectors(self, param, metadata=None):
        return self.router.connection(metadata=metadata).add_vectors(
            None, None, insert_param=param)

    @mark_grpc_method
    def Insert(self, request, context):
        logger.info('Insert')
        # TODO: Ths SDK interface add_vectors() could update, add a key 'row_id_array'
        _status, _ids = self._add_vectors(
            metadata={'resp_class': milvus_pb2.VectorIds}, param=request)
        return milvus_pb2.VectorIds(status=status_pb2.Status(
            error_code=_status.code, reason=_status.message),
            vector_id_array=_ids)

    @mark_grpc_method
    def Search(self, request, context):

        metadata = {'resp_class': milvus_pb2.TopKQueryResult}

        collection_name = request.collection_name

        topk = request.topk

        if len(request.extra_params) == 0:
            raise exceptions.SearchParamError(message="Search parma loss", metadata=metadata)
        params = ujson.loads(str(request.extra_params[0].value))

        logger.info('Search {}: topk={} params={}'.format(
            collection_name, topk, params))

        # if nprobe > self.MAX_NPROBE or nprobe <= 0:
        #     raise exceptions.InvalidArgumentError(
        #         message='Invalid nprobe: {}'.format(nprobe), metadata=metadata)

        if topk > self.MAX_TOPK or topk <= 0:
            raise exceptions.InvalidTopKError(
                message='Invalid topk: {}'.format(topk), metadata=metadata)

        collection_meta = self.collection_meta.get(collection_name, None)

        if not collection_meta:
            status, info = self.router.connection(
                metadata=metadata).describe_collection(collection_name)
            if not status.OK():
                raise exceptions.CollectionNotFoundError(collection_name,
                                                    metadata=metadata)

            self.collection_meta[collection_name] = info
            collection_meta = info

        start = time.time()

        query_record_array = []
        if int(collection_meta.metric_type) >= MetricType.HAMMING.value:
            for query_record in request.query_record_array:
                query_record_array.append(bytes(query_record.binary_data))
        else:
            for query_record in request.query_record_array:
                query_record_array.append(list(query_record.float_data))

        status, id_results, dis_results = self._do_query(context,
                                                         collection_name,
                                                         collection_meta,
                                                         query_record_array,
                                                         topk,
                                                         params,
                                                         partition_tags=getattr(request, "partition_tag_array", []),
                                                         metadata=metadata)

        now = time.time()
        logger.info('SearchVector takes: {}'.format(now - start))

        topk_result_list = milvus_pb2.TopKQueryResult(
            status=status_pb2.Status(error_code=status.error_code,
                                     reason=status.reason),
            row_num=len(request.query_record_array) if len(id_results) else 0,
            ids=id_results,
            distances=dis_results)
        return topk_result_list

    @mark_grpc_method
    def SearchInFiles(self, request, context):
        raise NotImplemented()

    def _describe_collection(self, collection_name, metadata=None):
        return self.router.connection(metadata=metadata).describe_collection(collection_name)

    @mark_grpc_method
    def DescribeCollection(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            return milvus_pb2.CollectionSchema(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message), )

        metadata = {'resp_class': milvus_pb2.CollectionSchema}

        logger.info('DescribeCollection {}'.format(_collection_name))
        _status, _collection = self._describe_collection(metadata=metadata,
                                               collection_name=_collection_name)

        if _status.OK():
            return milvus_pb2.CollectionSchema(
                collection_name=_collection_name,
                index_file_size=_collection.index_file_size,
                dimension=_collection.dimension,
                metric_type=_collection.metric_type,
                status=status_pb2.Status(error_code=_status.code,
                                         reason=_status.message),
            )

        return milvus_pb2.CollectionSchema(
            collection_name=_collection_name,
            status=status_pb2.Status(error_code=_status.code,
                                     reason=_status.message),
        )

    def _collection_info(self, collection_name, metadata=None):
        return self.router.connection(metadata=metadata).collection_info(collection_name)

    @mark_grpc_method
    def ShowCollectionInfo(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            return milvus_pb2.CollectionInfo(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message), )

        metadata = {'resp_class': milvus_pb2.CollectionInfo}

        logger.info('ShowCollectionInfo {}'.format(_collection_name))
        _status, _info = self._collection_info(metadata=metadata, collection_name=_collection_name)

        if _status.OK():
            _collection_info = milvus_pb2.CollectionInfo(
                status=status_pb2.Status(error_code=_status.code,
                                         reason=_status.message),
                total_row_count=_info.count
            )

            for par_stat in _info.partitions_stat:
                _par = milvus_pb2.PartitionStat(
                    tag=par_stat.tag,
                    total_row_count=par_stat.count
                )
                for seg_stat in par_stat.segments_stat:
                    _par.segments_stat.add(
                        segment_name=seg_stat.segment_name,
                        row_count=seg_stat.count,
                        index_name=seg_stat.index_name,
                        data_size=seg_stat.data_size,
                    )

                _collection_info.partitions_stat.append(_par)
            return _collection_info

        return milvus_pb2.CollectionInfo(
            status=status_pb2.Status(error_code=_status.code,
                                     reason=_status.message),
        )

    def _count_collection(self, collection_name, metadata=None):
        return self.router.connection(
            metadata=metadata).count_collection(collection_name)

    @mark_grpc_method
    def CountCollection(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            status = status_pb2.Status(error_code=_status.code,
                                       reason=_status.message)

            return milvus_pb2.CollectionRowCount(status=status)

        logger.info('CountCollection {}'.format(_collection_name))

        metadata = {'resp_class': milvus_pb2.CollectionRowCount}
        _status, _count = self._count_collection(_collection_name, metadata=metadata)

        return milvus_pb2.CollectionRowCount(
            status=status_pb2.Status(error_code=_status.code,
                                     reason=_status.message),
            collection_row_count=_count if isinstance(_count, int) else -1)

    def _get_server_version(self, metadata=None):
        return self.router.connection(metadata=metadata).server_version()

    def _cmd(self, cmd, metadata=None):
        return self.router.connection(metadata=metadata)._cmd(cmd)

    @mark_grpc_method
    def Cmd(self, request, context):
        _status, _cmd = Parser.parse_proto_Command(request)
        logger.info('Cmd: {}'.format(_cmd))

        if not _status.OK():
            return milvus_pb2.StringReply(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message))

        metadata = {'resp_class': milvus_pb2.StringReply}

        if _cmd == 'conn_stats':
            stats = self.router.readonly_topo.stats()
            return milvus_pb2.StringReply(status=status_pb2.Status(
                error_code=status_pb2.SUCCESS),
                string_reply=json.dumps(stats, indent=2))

        # if _cmd == 'version':
        #     _status, _reply = self._get_server_version(metadata=metadata)
        # else:
        #     _status, _reply = self.router.connection(
        #         metadata=metadata).server_status()
        _status, _reply = self._cmd(_cmd, metadata=metadata)

        return milvus_pb2.StringReply(status=status_pb2.Status(
            error_code=_status.code, reason=_status.message),
            string_reply=_reply)

    def _show_collections(self, metadata=None):
        return self.router.connection(metadata=metadata).show_collections()

    @mark_grpc_method
    def ShowCollections(self, request, context):
        logger.info('ShowCollections')
        metadata = {'resp_class': milvus_pb2.CollectionName}
        _status, _results = self._show_collections(metadata=metadata)

        return milvus_pb2.CollectionNameList(status=status_pb2.Status(
            error_code=_status.code, reason=_status.message),
            collection_names=_results)

    def _preload_collection(self, collection_name):
        return self.router.connection().preload_collection(collection_name)

    @mark_grpc_method
    def PreloadCollection(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        logger.info('PreloadCollection {}'.format(_collection_name))
        _status = self._preload_collection(_collection_name)
        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    def _describe_index(self, collection_name, metadata=None):
        return self.router.connection(metadata=metadata).describe_index(collection_name)

    @mark_grpc_method
    def DescribeIndex(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            return milvus_pb2.IndexParam(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message))

        metadata = {'resp_class': milvus_pb2.IndexParam}

        logger.info('DescribeIndex {}'.format(_collection_name))
        _status, _index_param = self._describe_index(collection_name=_collection_name,
                                                     metadata=metadata)

        if not _index_param:
            return milvus_pb2.IndexParam(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message))

        _index_type = _index_param._index_type

        grpc_index = milvus_pb2.IndexParam(status=status_pb2.Status(
            error_code=_status.code, reason=_status.message),
            collection_name=_collection_name, index_type=_index_type)

        grpc_index.extra_params.add(key='params', value=ujson.dumps(_index_param._params))
        return grpc_index

    def _get_vector_by_id(self, collection_name, vec_id, metadata):
        return self.router.connection(metadata=metadata).get_vector_by_id(collection_name, vec_id)

    @mark_grpc_method
    def GetVectorByID(self, request, context):
        _status, unpacks = Parser.parse_proto_VectorIdentity(request)
        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        metadata = {'resp_class': milvus_pb2.VectorData}

        _collection_name, _id = unpacks
        logger.info('GetVectorByID {}'.format(_collection_name))
        _status, vector = self._get_vector_by_id(_collection_name, _id, metadata)

        if not vector:
            return milvus_pb2.VectorData(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message), )

        if isinstance(vector, bytes):
            records = milvus_pb2.RowRecord(binary_data=vector)
        else:
            records = milvus_pb2.RowRecord(float_data=vector)

        return milvus_pb2.VectorData(status=status_pb2.Status(
            error_code=_status.code, reason=_status.message),
            vector_data=records
        )

    def _get_vector_ids(self, collection_name, segment_name, metadata):
        return self.router.connection(metadata=metadata).get_vector_ids(collection_name, segment_name)

    @mark_grpc_method
    def GetVectorIDs(self, request, context):
        _status, unpacks = Parser.parse_proto_GetVectorIDsParam(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        metadata = {'resp_class': milvus_pb2.VectorIds}

        _collection_name, _segment_name = unpacks
        logger.info('GetVectorIDs {}'.format(_collection_name))
        _status, ids = self._get_vector_ids(_collection_name, _segment_name, metadata)

        if not ids:
            return milvus_pb2.VectorIds(status=status_pb2.Status(
                error_code=_status.code, reason=_status.message), )

        return milvus_pb2.VectorIds(status=status_pb2.Status(
            error_code=_status.code, reason=_status.message),
            vector_id_array=ids
        )

    def _delete_by_id(self, collection_name, id_array):
        return self.router.connection().delete_by_id(collection_name, id_array)

    @mark_grpc_method
    def DeleteByID(self, request, context):
        _status, unpacks = Parser.parse_proto_DeleteByIDParam(request)

        if not _status.OK():
            logging.error('DeleteByID {}'.format(_status.message))
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        _collection_name, _ids = unpacks
        logger.info('DeleteByID {}'.format(_collection_name))
        _status = self._delete_by_id(_collection_name, _ids)

        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    def _drop_index(self, collection_name):
        return self.router.connection().drop_index(collection_name)

    @mark_grpc_method
    def DropIndex(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        logger.info('DropIndex {}'.format(_collection_name))
        _status = self._drop_index(_collection_name)
        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    def _flush(self, collection_names):
        return self.router.connection().flush(collection_names)

    @mark_grpc_method
    def Flush(self, request, context):
        _status, _collection_names = Parser.parse_proto_FlushParam(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        logger.info('Flush {}'.format(_collection_names))
        _status = self._flush(_collection_names)
        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)

    def _compact(self, collection_name):
        return self.router.connection().compact(collection_name)

    @mark_grpc_method
    def Compact(self, request, context):
        _status, _collection_name = Parser.parse_proto_CollectionName(request)

        if not _status.OK():
            return status_pb2.Status(error_code=_status.code,
                                     reason=_status.message)

        logger.info('Compact {}'.format(_collection_name))
        _status = self._compact(_collection_name)
        return status_pb2.Status(error_code=_status.code,
                                 reason=_status.message)
