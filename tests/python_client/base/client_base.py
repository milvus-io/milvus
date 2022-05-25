from numpy.core.fromnumeric import _partition_dispatcher
import pytest
import sys
from pymilvus import DefaultConfig

sys.path.append("..")
from base.connections_wrapper import ApiConnectionsWrapper
from base.collection_wrapper import ApiCollectionWrapper
from base.partition_wrapper import ApiPartitionWrapper
from base.index_wrapper import ApiIndexWrapper
from base.utility_wrapper import ApiUtilityWrapper
from base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct


class Base:
    """ Initialize class object """
    connection_wrap = None
    collection_wrap = None
    partition_wrap = None
    index_wrap = None
    utility_wrap = None
    collection_schema_wrap = None
    field_schema_wrap = None
    collection_object_list = []

    def setup_class(self):
        log.info("[setup_class] Start setup class...")

    def teardown_class(self):
        log.info("[teardown_class] Start teardown class...")

    def setup_method(self, method):
        log.info(("*" * 35) + " setup " + ("*" * 35))
        log.info("[setup_method] Start setup test case %s." % method.__name__)
        self.connection_wrap = ApiConnectionsWrapper()
        self.utility_wrap = ApiUtilityWrapper()
        self.collection_wrap = ApiCollectionWrapper()
        self.partition_wrap = ApiPartitionWrapper()
        self.index_wrap = ApiIndexWrapper()
        self.collection_schema_wrap = ApiCollectionSchemaWrapper()
        self.field_schema_wrap = ApiFieldSchemaWrapper()

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)

        try:
            """ Drop collection before disconnect """
            if not self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[0]:
                self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=cf.param_info.param_host,
                                             port=cf.param_info.param_port)

            if self.collection_wrap.collection is not None:
                if self.collection_wrap.collection.name.startswith("alias"):
                    log.info(f"collection {self.collection_wrap.collection.name} is alias, skip drop operation")
                else:
                    self.collection_wrap.drop(check_task=ct.CheckTasks.check_nothing)

            collection_list = self.utility_wrap.list_collections()[0]
            for collection_object in self.collection_object_list:
                if collection_object.collection is not None and collection_object.name in collection_list:
                    collection_object.drop(check_task=ct.CheckTasks.check_nothing)

        except Exception as e:
            log.debug(str(e))

        try:
            """ Delete connection and reset configuration"""
            res = self.connection_wrap.list_connections()
            for i in res[0]:
                self.connection_wrap.remove_connection(i[0])

            # because the connection is in singleton mode, it needs to be restored to the original state after teardown
            self.connection_wrap.add_connection(default={"host": DefaultConfig.DEFAULT_HOST,
                                                         "port": DefaultConfig.DEFAULT_PORT})
        except Exception as e:
            log.debug(str(e))


class TestcaseBase(Base):
    """
    Additional methods;
    Public methods that can be used for test cases.
    """

    def _connect(self):
        """ Add a connection and create the connect """
        res, is_succ = self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=cf.param_info.param_host,
                                                    port=cf.param_info.param_port)
        return res

    def init_collection_wrap(self, name=None, schema=None, shards_num=2, check_task=None, check_items=None, **kwargs):
        name = cf.gen_unique_str('coll_') if name is None else name
        schema = cf.gen_default_collection_schema() if schema is None else schema
        if not self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[0]:
            self._connect()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=name, schema=schema, shards_num=shards_num, check_task=check_task, check_items=check_items, **kwargs)
        self.collection_object_list.append(collection_w)
        return collection_w

    def init_multi_fields_collection_wrap(self, name=cf.gen_unique_str()):
        vec_fields = [cf.gen_float_vec_field(ct.another_float_vec_field_name)]
        schema = cf.gen_schema_multi_vector_fields(vec_fields)
        collection_w = self.init_collection_wrap(name=name, schema=schema)
        df = cf.gen_dataframe_multi_vec_fields(vec_fields=vec_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        return collection_w, df

    def init_partition_wrap(self, collection_wrap=None, name=None, description=None,
                            check_task=None, check_items=None, **kwargs):
        name = cf.gen_unique_str("partition_") if name is None else name
        description = cf.gen_unique_str("partition_des_") if description is None else description
        collection_wrap = self.init_collection_wrap() if collection_wrap is None else collection_wrap
        partition_wrap = ApiPartitionWrapper()
        partition_wrap.init_partition(collection_wrap.collection, name, description,
                                      check_task=check_task, check_items=check_items,
                                      **kwargs)
        return partition_wrap

    def init_collection_general(self, prefix, insert_data=False, nb=ct.default_nb,
                                partition_num=0, is_binary=False, is_all_data_type=False,
                                auto_id=False, dim=ct.default_dim, is_index=False, primary_field=ct.default_int64_field_name):
        """
        target: create specified collections
        method: 1. create collections (binary/non-binary, default/all data type, auto_id or not)
                2. create partitions if specified
                3. insert specified (binary/non-binary, default/all data type) data
                   into each partition if any
                4. not load if specifying is_index as True
        expected: return collection and raw data, insert ids
        """
        log.info("Test case of search interface: initialize before test case")
        self._connect()
        collection_name = cf.gen_unique_str(prefix)
        vectors = []
        binary_raw_vectors = []
        insert_ids = []
        time_stamp = 0
        # 1 create collection
        default_schema = cf.gen_default_collection_schema(auto_id=auto_id, dim=dim, primary_field=primary_field)
        if is_binary:
            default_schema = cf.gen_default_binary_collection_schema(auto_id=auto_id, dim=dim, primary_field=primary_field)
        if is_all_data_type:
            default_schema = cf.gen_collection_schema_all_datatype(auto_id=auto_id, dim=dim, primary_field=primary_field)
        log.info("init_collection_general: collection creation")
        collection_w = self.init_collection_wrap(name=collection_name,
                                                 schema=default_schema)
        # 2 add extra partitions if specified (default is 1 partition named "_default")
        if partition_num > 0:
            cf.gen_partitions(collection_w, partition_num)
        # 3 insert data if specified
        if insert_data:
            collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp = \
                cf.insert_data(collection_w, nb, is_binary, is_all_data_type, auto_id=auto_id, dim=dim)
            assert collection_w.is_empty is False
            assert collection_w.num_entities == nb
            log.info("insert_data: inserted data into collection %s (num_entities: %s)"
                     % (collection_w.name, nb))
            # This condition will be removed after auto index feature
            if not is_index:
                collection_w.load()

        return collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp

    def insert_entities_into_two_partitions_in_half(self, half, prefix='query'):
        """
        insert default entities into two partitions(partition_w and _default) in half(int64 and float fields values)
        :param half: half of nb
        :return: collection wrap and partition wrap
        """
        self._connect()
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)
        # insert [0, half) into partition_w
        df_partition = cf.gen_default_dataframe_data(nb=half, start=0)
        partition_w.insert(df_partition)
        # insert [half, nb) into _default
        df_default = cf.gen_default_dataframe_data(nb=half, start=half)
        collection_w.insert(df_default)
        # flush
        collection_w.num_entities
        collection_w.load(partition_names=[partition_w.name, "_default"])
        return collection_w, partition_w, df_partition, df_default

    def collection_insert_multi_segments_one_shard(self, collection_prefix, num_of_segment=2, nb_of_segment=1,
                                                   is_dup=True):
        """
        init collection with one shard, insert data into two segments on one shard (they can be merged)
        :param collection_prefix: collection name prefix
        :param num_of_segment: number of segments
        :param nb_of_segment: number of entities per segment
        :param is_dup: whether the primary keys of each segment is duplicated
        :return: collection wrap and partition wrap
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(collection_prefix), shards_num=1)

        for i in range(num_of_segment):
            start = 0 if is_dup else i * nb_of_segment
            df = cf.gen_default_dataframe_data(nb_of_segment, start=start)
            collection_w.insert(df)
            assert collection_w.num_entities == nb_of_segment * (i + 1)
        return collection_w
