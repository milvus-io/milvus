# import pytest
# from base.client_request import ApiReq
# from common.common_type import *
# from common.common_func import *
#
# prefix = "prefix_"
#
#
# class TestPartitionLoad(ApiReq):
#     """ Test case of partition load interface """
#
#     @pytest.mark.tags(CaseLabel.L1)
#     @pytest.mark.parametrize("fields", [
#         [],
#         ["filed_nonexistent"],
#         [default_int64_field_name],
#         [default_float_vec_field_name],
#         [default_int64_field_name, default_float_vec_field_name],
#         ["non_vector_field1", "non_vector_field2", "vector_field1", "vector_field2"]
#     ])
#     @pytest.mark.parametrize("indexes", [
#         [],
#         ["index_nonexistent"],
#         ["index1_default_int64_field_name"],
#         ["index_default_float_vec_field_name"],
#         ["index1_default_int64_field_name", "index2_default_float_vec_field_name"],
#         ["non_vector_index1", "non_vector_index2", "vector_index1", "vector_index2"]
#     ])
#     def test_partition_load_fields_and_indexes(self, fields, indexes):
#         """
#         target: verify load a partition with field and index
#         method: 1. create a collection with multi-fields and indexes
#                 2. create a partition and insert some data
#                 3. create index
#                 4. partition.load(fields, indexes)
#         expected: 1. load all fields and no indexes
#         """
#         """
#         m_collection = self._collection()
#         p_name = gen_unique_str(prefix)
#         m_partition, _ = self.partition.partition_init(m_collection, p_name)
#         # TODO: insert some data and flush
#         # m_partition.insert()
#         # m_collection.flush()
#         for i in range(len(indexes)):
#             m_collection.create_index(default_float_vec_field_name,
#                                       gen_simple_index()[i], index_name=indexes[i])
#         m_partition.load(field_names=fields, index_names=indexes)
#         # TODO: verify fields and indexes loaded accordingly
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L1)
#     def test_partition_load_only_non_vector(self):
#         """
#         target: verify load only a non vector field
#         method: 1. create a collection with multi-fields and indexes
#                 2. create a partition and insert some data
#                 3. create index
#                 4. partition.load(non_vector_field, none)
#         expected: 1. load all fields and no indexes
#         """
#         pass  # maybe covered by test_partition_load_fields_and_indexes
#
#     @pytest.mark.tags(CaseLabel.L1)
#     @pytest.mark.parametrize("indexed", [True, False])
#     @pytest.mark.parametrize("index_param", gen_simple_index())
#     def test_partition_load_empty_partition(self, indexed, index_param):
#         """
#         target: verify load an empty partition
#         method: 1.create an empty partition
#                 2. load the partition
#         expected: load successfully?
#         """
#         m_collection = self._collection()
#         p_name = gen_unique_str(prefix)
#         m_partition, _ = self.partition.partition_init(m_collection, p_name)
#         assert m_partition.is_empty
#         if indexed:
#             index_name = gen_unique_str()
#             m_collection.create_index(default_float_vec_field_name,
#                                       index_param,
#                                       index_name=index_name)
#             m_partition.load(default_float_vec_field_name, index_name)
#         else:
#             m_partition.load(default_float_vec_field_name)
#         # TODO: assert load successfully, but no data actually loaded
#
#     @pytest.mark.tags(CaseLabel.L1)
#     def test_partition_load_dropped_partition(self):
#         """
#         target: verify load an dropped partition
#         method: 1.create a partition
#                 2. drop the partition
#                 3. load the partition
#         expected: raise exception
#         """
#         m_partition = self._partition()
#         m_partition.load()
#         m_partition.drop()
#         with pytest.raises(Exception) as e:
#             m_partition.load()
#             # TODO assert the error code
#
#     @pytest.mark.tags(CaseLabel.L1)
#     def test_partition_load_dropped_collection(self):
#         """
#         target: verify load an dropped collection
#         method: 1.create a collection and partition
#                 2. drop the collection
#                 3. load the partition
#         expected: raise exception
#         """
#         m_collection = self._collection()
#         p_name = gen_unique_str(prefix)
#         m_partition, _ = self.partition.partition_init(m_collection, p_name)
#         assert m_collection.has_partition(p_name)
#         m_collection.drop()
#         with pytest.raises(Exception) as e:
#             m_partition.load()
#             # TODO assert the error code
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_partition_load_maximum_memory(self):
#         """
#         target: verify load many partitions that exceed the memory
#         method: 1.create a collection and many partitions
#                 2. insert huge amounts of data into partitions
#                 3. load the partitions
#         expected: raise exception
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L1)
#     def test_partition_load_partition_twice(self):
#         """
#         target: verify load the same partition twice
#         method: 1.create a partition
#                 2. insert some data into the partition
#                 3. load the partition
#                 4. load the partition again
#         expected: both loading succeed, no more memory consumed at 2nd load
#         """
#         m_partition = self._partition()
#         m_partition.insert(gen_default_list_data())
#         m_partition.load()
#         # TODO: assert load successfully
#         m_partition.load()
#         # TODO: assert no more memory consumed
#
#
# class TestCollectionLoad(ApiReq):
#     """ Test case of collection load interface """
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_case(self):
#         log.info("Test case of load interface")
