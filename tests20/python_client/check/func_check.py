from utils.util_log import test_log as log
from common.common_type import *


class CheckFunc:
    def __init__(self, res, func_name, check_res, **kwargs):
        self.res = res  # response of api request
        self.func_name = func_name
        self.check_res = check_res
        self.params = {}

        for key, value in kwargs.items():
            self.params[key] = value

        self.keys = self.params.keys()

    def run(self):
        check_result = True

        if self.check_res is None:
            pass
            # log.info("self.check_res is None, the response of API: %s" % self.res)
        elif self.check_res == cname_param_check:
            check_result = self.req_cname_check(self.res, self.func_name, self.params.get('collection_name'))
        elif self.check_res == pname_param_check:
            check_result = self.req_pname_check(self.res, self.func_name, self.params.get('partition_tag'))

        return check_result

    @staticmethod
    def req_cname_check(res, func_name, params):
        code = getattr(res, 'code', "The exception does not contain the field of code.")
        message = getattr(res, 'message', "The exception does not contain the field of message.")

        if not isinstance(params, str):
            log.info("[req_params_check] Check param is not a str.")
            if func_name in ["drop_collection", "has_collection", "describe_collection", "load_collection",
                             "release_collection", "list_partitions", "create_partition", "drop_partition",
                             "has_partition", "load_partitions", "release_partitions", "drop_index", "describe_index"]:
                assert res.args[0] == "`collection_name` value %s is illegal" % str(params)
            elif func_name in ["create_collection", "create_index", "search"]:
                if params is None:
                    if func_name in ["search"]:
                        assert code == 1
                        assert message == "collection not exists"
                    else:
                        assert code == 1
                        assert message == "Collection name should not be empty"
                else:
                    # check_str = "Cannot set milvus.proto.schema.CollectionSchema.name to" + \
                    #             " %s: %s has type %s," % (str(params), str(params), str(type(params))) +\
                    #             " but expected one of: (<class 'bytes'>, <class 'str'>) for field CollectionSchema.name"
                    # assert res.args[0] == check_str
                    check_str = "%s has type %s, but expected one of: bytes, unicode" % (str(params), str(type(params))[8:-2])
                    assert res.args[0] == check_str
            # elif func_name == "create_index":
            #     if params is None:
            #         assert code == 1
            #         assert message == "Collection name should not be empty"
            #     else:
            #         check_str = "Cannot set milvus.proto.milvus.DescribeCollectionRequest.collection_name to" + \
            #                     " %s: %s has type %s," % (str(params), str(params), str(type(params))) +\
            #                     " but expected one of: (<class 'bytes'>, <class 'str'>) for field DescribeCollectionRequest.collection_name"
            #         assert res.args[0] == check_str
            # elif func_name == "search":
            #     if params is None:
            #         assert code == 1
            #         assert message == "collection not exists"
            #     else:
            #         check_str = "Cannot set milvus.proto.milvus.HasCollectionRequest.collection_name to" + \
            #                     " %s: %s has type %s," % (str(params), str(params), str(type(params))) +\
            #                     " but expected one of: (<class 'bytes'>, <class 'str'>) for field HasCollectionRequest.collection_name"
            #         assert res.args[0] == check_str
            elif func_name == "flush":
                if params is None or params == []:
                    assert res.args[0] == "Collection name list can not be None or empty"
                elif params == [1, "2", 3]:
                    assert res.args[0] == "`collection_name` value 1 is illegal"
                else:
                    assert res.args[0] == "Collection name array must be type of list"

        elif isinstance(params, str) and len(params) < 256:
            log.info("[req_params_check] Check str param less than 256.")
            if func_name in ["create_collection", "drop_collection", "has_collection", "describe_collection",
                             "load_collection", "release_collection", "list_partitions", "create_partition",
                             "drop_partition", "has_partition", "load_partitions", "release_partitions", "drop_index", "describe_index"]:
                assert code == 1
                assert message == "Invalid collection name: %s. The first character of a collection name must be an underscore or letter." % params
            elif func_name == "flush":
                assert res.args[0] == "Collection name array must be type of list"
            elif func_name == "create_index":
                assert code == 1
                assert message == "Invalid collection name: %s. The first character of a collection name must be an underscore or letter." % params
            elif func_name == "search":
                assert code == 1
                assert message == "collection not exists"

        elif isinstance(params, str) and len(params) >= 256:
            log.info("[req_params_check] Check str param more than 256.")
            if func_name in ["create_collection", "drop_collection", "has_collection", "describe_collection",
                             "load_collection", "release_collection", "list_partitions", "create_partition",
                             "drop_partition", "has_partition", "load_partitions", "release_partitions", "drop_index",
                             "describe_index", "create_index"]:
                assert code == 1
                assert message == "Invalid collection name: %s. The length of a collection name must be less than 255 characters." % params
            elif func_name == "flush":
                assert res.args[0] == "Collection name array must be type of list"
            elif func_name == "search":
                assert code == 1
                assert message == "collection not exists"

        return True

    @staticmethod
    def req_pname_check(res, func_name, params):
        code = getattr(res, 'code', "The exception does not contain the field of code.")
        message = getattr(res, 'message', "The exception does not contain the field of message.")

        if not isinstance(params, str):
            log.info("[req_pname_check] Check param is not a str.")
            log.info(res.args[0])
            if func_name in ["create_partition", "drop_partition", "has_partition"]:
                assert res.args[0] == "`partition_tag` value %s is illegal" % str(params)

        elif isinstance(params, str) and len(params) < 256:
            log.info("[req_pname_check] Check str param less than 256.")
            if func_name in ["create_partition", "drop_partition", "has_partition"]:
                assert code == 1
                check_str = ["Invalid partition tag: %s. Partition tag can only contain numbers, letters, dollars and underscores." % str(params),
                             "Invalid partition tag: %s. The first character of a partition tag must be an underscore or letter." % str(params)]
                assert message in check_str

        elif isinstance(params, str) and len(params) >= 256:
            log.info("[req_pname_check] Check str param more than 256.")
            if func_name in ["create_partition", "drop_partition", "has_partition"]:
                assert code == 1
                assert message == "Invalid partition tag: %s. The length of a partition tag must be less than 255 characters." % str(params)

        return True
