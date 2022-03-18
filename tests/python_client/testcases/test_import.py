# import time
# import pytest
#
# from base.client_base import TestcaseBase
# from common import common_func as cf
# from common import common_type as ct
# from common.common_type import CaseLabel
# from utils.util_log import test_log as log
#
#
# class TestRowBasedImport(TestcaseBase):
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_auto_id_float_vector(self):
#         """
#         collection: auto_id
#         import data: float vectors
#         """
#         files = None  # TODO: when data file ready
#         self._connect()
#         c_name = cf.gen_unique_str()
#         fields = [cf.gen_float_field()]
#         schema = cf.gen_collection_schema(fields=fields, auto_id=True)
#         self.collection_wrap.init_collection(c_name, schema=schema)
#         task_ids, = self.utility_wrap.import_data(collection_name=c_name, files=files)
#         for task_id in task_ids:
#             self.utility_wrap.get_import_state(task_id)
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_auto_id_partition_float_vector(self):
#         """
#         collection: auto_id and customized partitions
#         import data: float vectors
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_auto_id_binary_vector(self):
#         """
#         collection: auto_id
#         import data: binary vectors
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_auto_id_float_vector_multi_scalars_twice(self):
#         """
#         collection: auto_id
#         import data: float vectors and multiple scalars with diff dtypes
#         import twice
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_auto_id_binary_vector_string_scalar(self):
#         """
#         collection: auto_id
#         import data: binary vectors and string scalar
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_float_vector_string_primary(self):
#         """
#         collection: custom_id
#         import data: float vectors and string primary key
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_float_partition_vector_string_primary(self):
#         """
#         collection: custom_id and custom partition
#         import data: float vectors and string primary key
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_binary_vector_int_primary_from_bucket(self):
#         """
#         collection: custom_id
#         import data: binary vectors and int primary key
#         import from a particular bucket
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_binary_vector_string_primary_multi_scalars_twice(self):
#         """
#         collection: custom_id
#         import data: binary vectors, string primary key and multiple scalars
#         import twice
#         """
#         pass
#
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_custom_id_float_vector_int_primary_multi_scalars_twice(self):
#         """
#         collection: custom_id
#         import data: float vectors, int primary key and multiple scalars
#         import twice
#         """
#         pass
#
#
# class TestColumnBasedImport(TestcaseBase):
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_default(self):
#         pass
#
#
# class TestImportInvalidParams(TestcaseBase):
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_default(self):
#         pass
#
#
# class TestImportAdvanced(TestcaseBase):
#     """Validate data consistency and availability during import"""
#     @pytest.mark.tags(CaseLabel.L3)
#     def test_default(self):
#         pass
#
