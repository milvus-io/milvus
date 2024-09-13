import pytest
import sys
from typing import Dict, List
from pymilvus import DefaultConfig

from base.database_wrapper import ApiDatabaseWrapper

sys.path.append("..")
from base.connections_wrapper import ApiConnectionsWrapper
from base.collection_wrapper import ApiCollectionWrapper
from base.partition_wrapper import ApiPartitionWrapper
from base.index_wrapper import ApiIndexWrapper
from base.utility_wrapper import ApiUtilityWrapper
from base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from base.high_level_api_wrapper import HighLevelApiWrapper
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_params import IndexPrams

from pymilvus import ResourceGroupInfo, DataType


class Base:
    """ Initialize class object """
    connection_wrap = None
    collection_wrap = None
    partition_wrap = None
    index_wrap = None
    utility_wrap = None
    collection_schema_wrap = None
    field_schema_wrap = None
    database_wrap = None
    collection_object_list = []
    resource_group_list = []
    high_level_api_wrap = None
    skip_connection = False

    def setup_class(self):
        log.info("[setup_class] Start setup class...")

    def teardown_class(self):
        log.info("[teardown_class] Start teardown class...")

    def setup_method(self, method):
        log.info(("*" * 35) + " setup " + ("*" * 35))
        log.info("[setup_method] Start setup test case %s." % method.__name__)
        self._setup_objects()

    def _setup_objects(self):
        self.connection_wrap = ApiConnectionsWrapper()
        self.utility_wrap = ApiUtilityWrapper()
        self.collection_wrap = ApiCollectionWrapper()
        self.partition_wrap = ApiPartitionWrapper()
        self.index_wrap = ApiIndexWrapper()
        self.collection_schema_wrap = ApiCollectionSchemaWrapper()
        self.field_schema_wrap = ApiFieldSchemaWrapper()
        self.database_wrap = ApiDatabaseWrapper()
        self.high_level_api_wrap = HighLevelApiWrapper()

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)
        self._teardown_objects()

    def _teardown_objects(self):
        try:
            """ Drop collection before disconnect """
            if not self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[0]:
                self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=cf.param_info.param_host,
                                             port=cf.param_info.param_port, user=ct.default_user,
                                             password=ct.default_password)

            if self.collection_wrap.collection is not None:
                if self.collection_wrap.collection.name.startswith("alias"):
                    log.info(f"collection {self.collection_wrap.collection.name} is alias, skip drop operation")
                else:
                    self.collection_wrap.drop(check_task=ct.CheckTasks.check_nothing)

            collection_list = self.utility_wrap.list_collections()[0]
            for collection_object in self.collection_object_list:
                if collection_object.collection is not None and collection_object.name in collection_list:
                    collection_object.drop(check_task=ct.CheckTasks.check_nothing)

            """ Clean up the rgs before disconnect """
            rgs_list = self.utility_wrap.list_resource_groups()[0]
            for rg_name in self.resource_group_list:
                if rg_name is not None and rg_name in rgs_list:
                    rg = \
                    self.utility_wrap.describe_resource_group(name=rg_name, check_task=ct.CheckTasks.check_nothing)[0]
                    if isinstance(rg, ResourceGroupInfo):
                        if rg.num_available_node > 0:
                            self.utility_wrap.transfer_node(source=rg_name,
                                                            target=ct.default_resource_group_name,
                                                            num_node=rg.num_available_node)
                        self.utility_wrap.drop_resource_group(rg_name, check_task=ct.CheckTasks.check_nothing)

        except Exception as e:
            log.debug(str(e))

        try:
            """ Drop roles before disconnect """
            if not self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[0]:
                self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING, host=cf.param_info.param_host,
                                             port=cf.param_info.param_port, user=ct.default_user,
                                             password=ct.default_password)

            role_list = self.utility_wrap.list_roles(False)[0]
            for role in role_list.groups:
                role_name = role.role_name
                if role_name not in ["admin", "public"]:
                    each_role = self.utility_wrap.init_role(name=role_name)[0]
                    each_role.drop()

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

    def _connect(self, enable_milvus_client_api=False):
        """ Add a connection and create the connect """
        if self.skip_connection:
            return None

        if enable_milvus_client_api:
            if cf.param_info.param_uri:
                uri = cf.param_info.param_uri
            else:
                uri = "http://" + cf.param_info.param_host + ":" + str(cf.param_info.param_port)
            res, is_succ = self.connection_wrap.MilvusClient(uri=uri,
                                                             token=cf.param_info.param_token)
        else:
            if cf.param_info.param_user and cf.param_info.param_password:
                res, is_succ = self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING,
                                                            host=cf.param_info.param_host,
                                                            port=cf.param_info.param_port,
                                                            user=cf.param_info.param_user,
                                                            password=cf.param_info.param_password,
                                                            secure=cf.param_info.param_secure)
            else:
                res, is_succ = self.connection_wrap.connect(alias=DefaultConfig.DEFAULT_USING,
                                                            host=cf.param_info.param_host,
                                                            port=cf.param_info.param_port)

        return res

    def init_collection_wrap(self, name=None, schema=None, check_task=None, check_items=None,
                             enable_dynamic_field=False, with_json=True, **kwargs):
        name = cf.gen_unique_str('coll_') if name is None else name
        schema = cf.gen_default_collection_schema(enable_dynamic_field=enable_dynamic_field, with_json=with_json) \
            if schema is None else schema
        if not self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[0]:
            self._connect()
        collection_w = ApiCollectionWrapper()
        collection_w.init_collection(name=name, schema=schema, check_task=check_task,
                                     check_items=check_items, **kwargs)
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

    def insert_data_general(self, prefix="test", insert_data=False, nb=ct.default_nb,
                            partition_num=0, is_binary=False, is_all_data_type=False,
                            auto_id=False, dim=ct.default_dim,
                            primary_field=ct.default_int64_field_name, is_flush=True, name=None,
                            enable_dynamic_field=False, with_json=True, **kwargs):
        """

        """
        self._connect()
        collection_name = cf.gen_unique_str(prefix)
        if name is not None:
            collection_name = name
        vectors = []
        binary_raw_vectors = []
        insert_ids = []
        time_stamp = 0
        # 1 create collection
        default_schema = cf.gen_default_collection_schema(auto_id=auto_id, dim=dim, primary_field=primary_field,
                                                          enable_dynamic_field=enable_dynamic_field,
                                                          with_json=with_json)
        if is_binary:
            default_schema = cf.gen_default_binary_collection_schema(auto_id=auto_id, dim=dim,
                                                                     primary_field=primary_field)
        if is_all_data_type:
            default_schema = cf.gen_collection_schema_all_datatype(auto_id=auto_id, dim=dim,
                                                                   primary_field=primary_field,
                                                                   enable_dynamic_field=enable_dynamic_field,
                                                                   with_json=with_json)
        log.info("insert_data_general: collection creation")
        collection_w = self.init_collection_wrap(name=collection_name, schema=default_schema, **kwargs)
        pre_entities = collection_w.num_entities
        if insert_data:
            collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp = \
                cf.insert_data(collection_w, nb, is_binary, is_all_data_type, auto_id=auto_id, dim=dim,
                               enable_dynamic_field=enable_dynamic_field,
                               with_json=with_json)
            if is_flush:
                collection_w.flush()
                assert collection_w.num_entities == nb + pre_entities

        return collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp

    def init_collection_general(self, prefix="test", insert_data=False, nb=ct.default_nb,
                                partition_num=0, is_binary=False, is_all_data_type=False,
                                auto_id=False, dim=ct.default_dim, is_index=True,
                                primary_field=ct.default_int64_field_name, is_flush=True, name=None,
                                enable_dynamic_field=False, with_json=True, random_primary_key=False,
                                multiple_dim_array=[], is_partition_key=None, vector_data_type="FLOAT_VECTOR",
                                nullable_fields={}, default_value_fields={}, **kwargs):
        """
        target: create specified collections
        method: 1. create collections (binary/non-binary, default/all data type, auto_id or not)
                2. create partitions if specified
                3. insert specified (binary/non-binary, default/all data type) data
                   into each partition if any
                4. not load if specifying is_index as True
                5. enable insert null data: nullable_fields = {"nullable_fields_name": null data percent}
                6. enable insert default value: default_value_fields = {"default_fields_name": default value}
        expected: return collection and raw data, insert ids
        """
        log.info("Test case of search interface: initialize before test case")
        if not self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[0]:
            self._connect()
        collection_name = cf.gen_unique_str(prefix)
        if name is not None:
            collection_name = name
        if not isinstance(nullable_fields, dict):
            log.error("nullable_fields should a dict like {'nullable_fields_name': null data percent}")
            assert False
        if not isinstance(default_value_fields, dict):
            log.error("default_value_fields should a dict like {'default_fields_name': default value}")
            assert False
        vectors = []
        binary_raw_vectors = []
        insert_ids = []
        time_stamp = 0
        # 1 create collection
        default_schema = cf.gen_default_collection_schema(auto_id=auto_id, dim=dim, primary_field=primary_field,
                                                          enable_dynamic_field=enable_dynamic_field,
                                                          with_json=with_json, multiple_dim_array=multiple_dim_array,
                                                          is_partition_key=is_partition_key,
                                                          vector_data_type=vector_data_type,
                                                          nullable_fields=nullable_fields,
                                                          default_value_fields=default_value_fields)
        if is_binary:
            default_schema = cf.gen_default_binary_collection_schema(auto_id=auto_id, dim=dim,
                                                                     primary_field=primary_field,
                                                                     nullable_fields=nullable_fields,
                                                                     default_value_fields=default_value_fields)
        if vector_data_type == ct.sparse_vector:
            default_schema = cf.gen_default_sparse_schema(auto_id=auto_id, primary_field=primary_field,
                                                          enable_dynamic_field=enable_dynamic_field,
                                                          with_json=with_json,
                                                          multiple_dim_array=multiple_dim_array,
                                                          nullable_fields=nullable_fields,
                                                          default_value_fields=default_value_fields)
        if is_all_data_type:
            default_schema = cf.gen_collection_schema_all_datatype(auto_id=auto_id, dim=dim,
                                                                   primary_field=primary_field,
                                                                   enable_dynamic_field=enable_dynamic_field,
                                                                   with_json=with_json,
                                                                   multiple_dim_array=multiple_dim_array,
                                                                   nullable_fields=nullable_fields,
                                                                   default_value_fields=default_value_fields)
        log.info("init_collection_general: collection creation")
        collection_w = self.init_collection_wrap(name=collection_name, schema=default_schema, **kwargs)
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        # 2 add extra partitions if specified (default is 1 partition named "_default")
        if partition_num > 0:
            cf.gen_partitions(collection_w, partition_num)
        # 3 insert data if specified
        if insert_data:
            collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp = \
                cf.insert_data(collection_w, nb, is_binary, is_all_data_type, auto_id=auto_id,
                               dim=dim, enable_dynamic_field=enable_dynamic_field, with_json=with_json,
                               random_primary_key=random_primary_key, multiple_dim_array=multiple_dim_array,
                               primary_field=primary_field, vector_data_type=vector_data_type,
                               nullable_fields=nullable_fields)
            if is_flush:
                assert collection_w.is_empty is False
                assert collection_w.num_entities == nb
        # 4 create default index if specified
        if is_index:
            # This condition will be removed after auto index feature
            if is_binary:
                collection_w.create_index(ct.default_binary_vec_field_name, ct.default_bin_flat_index)
            elif vector_data_type == ct.sparse_vector:
                for vector_name in vector_name_list:
                    collection_w.create_index(vector_name, ct.default_sparse_inverted_index)
            else:
                if len(multiple_dim_array) == 0 or is_all_data_type == False:
                    vector_name_list.append(ct.default_float_vec_field_name)
                for vector_name in vector_name_list:
                    # Unlike dense vectors, sparse vectors cannot create flat index.
                    if ct.sparse_vector in vector_name:
                        collection_w.create_index(vector_name, ct.default_sparse_inverted_index)
                    else:
                        collection_w.create_index(vector_name, ct.default_flat_index)

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
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
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

    def init_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None, **kwargs):
        if not self.connection_wrap.has_connection(alias=DefaultConfig.DEFAULT_USING)[0]:
            self._connect()
        utility_w = ApiUtilityWrapper()
        res, check_result = utility_w.create_resource_group(name=name, using=using, timeout=timeout,
                                                            check_task=check_task,
                                                            check_items=check_items, **kwargs)
        if res is None and check_result:
            self.resource_group_list.append(name)
        return res, check_result

    def init_user_with_privilege(self, privilege_object, object_name, privilege, db_name="default"):
        """
        init a user and role, grant privilege to the role with the db, then bind the role to the user
        :param privilege_object: privilege object: Global, Collection, User
        :type privilege_object: str
        :param object_name: privilege object name
        :type object_name: str
        :param privilege: privilege
        :type privilege: str
        :param db_name: database name
        :type db_name: str
        :return: user name, user pwd, role name
        :rtype: str, str, str
        """
        tmp_user = cf.gen_unique_str("user")
        tmp_pwd = cf.gen_unique_str("pwd")
        tmp_role = cf.gen_unique_str("role")

        # create user
        self.utility_wrap.create_user(tmp_user, tmp_pwd)

        # create role
        self.utility_wrap.init_role(tmp_role)
        self.utility_wrap.create_role()

        # grant privilege to the role
        self.utility_wrap.role_grant(object=privilege_object, object_name=object_name, privilege=privilege,
                                     db_name=db_name)

        # bind the role to the user
        self.utility_wrap.role_add_user(tmp_user)

        return tmp_user, tmp_pwd, tmp_role

    def build_multi_index(self, index_params: Dict[str, IndexPrams], collection_obj: ApiCollectionWrapper = None):
        collection_obj = collection_obj or self.collection_wrap
        for k, v in index_params.items():
            collection_obj.create_index(field_name=k, index_params=v.to_dict, index_name=k)
        log.info(f"[TestcaseBase] Build all indexes done: {list(index_params.keys())}")
        return collection_obj

    def drop_multi_index(self, index_names: List[str], collection_obj: ApiCollectionWrapper = None,
                         check_task=None, check_items=None):
        collection_obj = collection_obj or self.collection_wrap
        for n in index_names:
            collection_obj.drop_index(index_name=n, check_task=check_task, check_items=check_items)
        log.info(f"[TestcaseBase] Drop all indexes done: {index_names}")
        return collection_obj

    def show_indexes(self, collection_obj: ApiCollectionWrapper = None):
        collection_obj = collection_obj or self.collection_wrap
        indexes = {n.field_name: n.params for n in self.collection_wrap.indexes}
        log.info("[TestcaseBase] Collection: `{0}` index: {1}".format(collection_obj.name, indexes))
        return indexes


class TestCaseClassBase(TestcaseBase):
    """
    Setup objects on class
    """

    def setup_class(self):
        log.info("[setup_class] " + " Start setup class ".center(100, "~"))
        self._setup_objects(self)

    def teardown_class(self):
        log.info("[teardown_class]" + " Start teardown class ".center(100, "~"))
        self._teardown_objects(self)

    def setup_method(self, method):
        log.info(" setup ".center(80, "*"))
        log.info("[setup_method] Start setup test case %s." % method.__name__)

    def teardown_method(self, method):
        log.info(" teardown ".center(80, "*"))
        log.info("[teardown_method] Start teardown test case %s..." % method.__name__)

    @property
    def all_scalar_fields(self):
        dtypes = [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.VARCHAR, DataType.BOOL,
                  DataType.FLOAT, DataType.DOUBLE]
        dtype_names = [f"{n.name}" for n in dtypes] + [f"ARRAY_{n.name}" for n in dtypes] + [DataType.JSON.name]
        return dtype_names

    @property
    def all_index_scalar_fields(self):
        return list(set(self.all_scalar_fields) - {DataType.JSON.name})

    @property
    def inverted_support_dtype_names(self):
        return self.all_index_scalar_fields

    @property
    def inverted_not_support_dtype_names(self):
        return [DataType.JSON.name]

    @property
    def bitmap_support_dtype_names(self):
        dtypes = [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.BOOL, DataType.VARCHAR]
        dtype_names = [f"{n.name}" for n in dtypes] + [f"ARRAY_{n.name}" for n in dtypes]
        return dtype_names

    @property
    def bitmap_not_support_dtype_names(self):
        return list(set(self.all_scalar_fields) - set(self.bitmap_support_dtype_names))
