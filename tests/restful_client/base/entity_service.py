from api.entity import Entity
from common import common_type as ct
from utils.util_log import test_log as log
from models import common, schema, milvus, server

TIMEOUT = 30


class EntityService:

    def __init__(self, endpoint=None, timeout=None):
        if timeout is None:
            timeout = TIMEOUT
        if endpoint is None:
            endpoint = "http://localhost:9091/api/v1"
        self._entity = Entity(endpoint=endpoint)

    def calc_distance(self, base=None, op_left=None, op_right=None, params=None):
        payload = {
            "base": base,
            "op_left": op_left,
            "op_right": op_right,
            "params": params
        }
        # payload = milvus.CalcDistanceRequest(base=base, op_left=op_left, op_right=op_right, params=params)
        # payload = payload.dict()
        return self._entity.calc_distance(payload)

    def delete(self, base=None, collection_name=None, db_name=None, expr=None, hash_keys=None, partition_name=None):
        payload = {
            "base": base,
            "collection_name": collection_name,
            "db_name": db_name,
            "expr": expr,
            "hash_keys": hash_keys,
            "partition_name": partition_name
        }
        # payload = server.DeleteRequest(base=base,
        #                                collection_name=collection_name,
        #                                db_name=db_name,
        #                                expr=expr,
        #                                hash_keys=hash_keys,
        #                                partition_name=partition_name)
        # payload = payload.dict()
        return self._entity.delete(payload)

    def insert(self, base=None, collection_name=None, db_name=None, fields_data=None, hash_keys=None, num_rows=None,
               partition_name=None, check_task=True):
        payload = {
            "base": base,
            "collection_name": collection_name,
            "db_name": db_name,
            "fields_data": fields_data,
            "hash_keys": hash_keys,
            "num_rows": num_rows,
            "partition_name": partition_name
        }
        # payload = milvus.InsertRequest(base=base,
        #                                collection_name=collection_name,
        #                                db_name=db_name,
        #                                fields_data=fields_data,
        #                                hash_keys=hash_keys,
        #                                num_rows=num_rows,
        #                                partition_name=partition_name)
        # payload = payload.dict()
        rsp = self._entity.insert(payload)
        if check_task:
            assert rsp["status"] == {}
            assert rsp["insert_cnt"] == num_rows
        return rsp

    def flush(self, base=None, collection_names=None, db_name=None, check_task=True):
        payload = {
            "base": base,
            "collection_names": collection_names,
            "db_name": db_name
        }
        # payload = server.FlushRequest(base=base,
        #                               collection_names=collection_names,
        #                               db_name=db_name)
        # payload = payload.dict()
        rsp = self._entity.flush(payload)
        if check_task:
            assert rsp["status"] == {}

    def get_persistent_segment_info(self, base=None, collection_name=None, db_name=None):
        payload = {
            "base": base,
            "collection_name": collection_name,
            "db_name": db_name
        }
        # payload = server.GetPersistentSegmentInfoRequest(base=base,
        #                                                  collection_name=collection_name,
        #                                                  db_name=db_name)
        # payload = payload.dict()
        return self._entity.get_persistent_segment_info(payload)

    def get_flush_state(self, segment_ids=None):
        payload = {
            "segment_ids": segment_ids
        }
        # payload = server.GetFlushStateRequest(segment_ids=segment_ids)
        # payload = payload.dict()
        return self._entity.get_flush_state(payload)

    def query(self, base=None, collection_name=None, db_name=None, expr=None,
              guarantee_timestamp=None, output_fields=None, partition_names=None, travel_timestamp=None,
              check_task=True):
        payload = {
            "base": base,
            "collection_name": collection_name,
            "db_name": db_name,
            "expr": expr,
            "guarantee_timestamp": guarantee_timestamp,
            "output_fields": output_fields,
            "partition_names": partition_names,
            "travel_timestamp": travel_timestamp
        }
        #
        # payload = server.QueryRequest(base=base, collection_name=collection_name, db_name=db_name, expr=expr,
        #                               guarantee_timestamp=guarantee_timestamp, output_fields=output_fields,
        #                               partition_names=partition_names, travel_timestamp=travel_timestamp)
        # payload = payload.dict()
        rsp = self._entity.query(payload)
        if check_task:
            fields_data = rsp["fields_data"]
            for field_data in fields_data:
                if field_data["field_name"] in expr:
                    data = field_data["Field"]["Scalars"]["Data"]["LongData"]["data"]
                    for d in data:
                        s = expr.replace(field_data["field_name"], str(d))
                        assert eval(s) is True
        return rsp

    def get_query_segment_info(self, base=None, collection_name=None, db_name=None):
        payload = {
            "base": base,
            "collection_name": collection_name,
            "db_name": db_name
        }
        # payload = server.GetQuerySegmentInfoRequest(base=base,
        #                                             collection_name=collection_name,
        #                                             db_name=db_name)
        # payload = payload.dict()
        return self._entity.get_query_segment_info(payload)

    def search(self, base=None, collection_name=None, vectors=None, db_name=None, dsl=None,
               output_fields=None, dsl_type=1,
               guarantee_timestamp=None, partition_names=None, placeholder_group=None,
               search_params=None, travel_timestamp=None, check_task=True):
        payload = {
            "base": base,
            "collection_name": collection_name,
            "output_fields": output_fields,
            "vectors": vectors,
            "db_name": db_name,
            "dsl": dsl,
            "dsl_type": dsl_type,
            "guarantee_timestamp": guarantee_timestamp,
            "partition_names": partition_names,
            "placeholder_group": placeholder_group,
            "search_params": search_params,
            "travel_timestamp": travel_timestamp
        }
        # payload = server.SearchRequest(base=base, collection_name=collection_name, db_name=db_name, dsl=dsl,
        #                                dsl_type=dsl_type, guarantee_timestamp=guarantee_timestamp,
        #                                partition_names=partition_names, placeholder_group=placeholder_group,
        #                                search_params=search_params, travel_timestamp=travel_timestamp)
        # payload = payload.dict()
        rsp = self._entity.search(payload)
        if check_task:
            assert rsp["status"] == {}
            assert rsp["results"]["num_queries"] == len(vectors)
            assert len(rsp["results"]["ids"]["IdField"]["IntId"]["data"]) == sum(rsp["results"]["topks"])
        return rsp







