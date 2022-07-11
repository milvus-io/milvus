import pytest
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from deploy.base import TestDeployBase
from deploy import common as dc
from deploy.common import gen_index_param, gen_search_param


default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
binary_field_name = default_binary_vec_field_name
default_search_exp = "int64 >= 0"
default_term_expr = f'{ct.default_int64_field_name} in [0, 1]'

class TestActionBeforeReinstall(TestDeployBase):
	""" Test case of action before reinstall """
	def teardown_method(self, method):
		log.info(("*" * 35) + " teardown " + ("*" * 35))
		log.info("[teardown_method] Start teardown test case %s..." % method.__name__)
		log.info("skip drop collection")

	@pytest.mark.skip()
	@pytest.mark.tags(CaseLabel.L3)
	@pytest.mark.parametrize("index_type", dc.all_index_types) #, "BIN_FLAT"
	def test_task_1(self, index_type, data_size):
		"""
        before reinstall: create collection and insert data, load and search
        after reinstall: get collection, load, search, create index, load, and search
		"""
		name = "task_1_" + index_type
		insert_data = True
		is_binary = True if "BIN" in index_type else False
		is_flush = False
		collection_w = self.init_collection_general(insert_data=insert_data, is_binary=is_binary, nb=data_size,
											        is_flush=is_flush, name=name)[0]
		collection_w.load()

		if is_binary:
			_, vectors_to_search = cf.gen_binary_vectors(default_nb, default_dim)
			default_search_field = ct.default_binary_vec_field_name
		else:
			vectors_to_search = cf.gen_vectors(default_nb, default_dim)
			default_search_field = ct.default_float_vec_field_name
		search_params = gen_search_param(index_type)[0]
		collection_w.search(vectors_to_search[:default_nq], default_search_field,
							search_params, default_limit,
							default_search_exp,
							check_task=CheckTasks.check_search_results,
							check_items={"nq": default_nq,
										 "limit": default_limit})
		output_fields = [ct.default_int64_field_name]
		collection_w.query(default_term_expr, output_fields=output_fields,
						   check_task=CheckTasks.check_query_not_empty)

	@pytest.mark.tags(CaseLabel.L3)
	@pytest.mark.parametrize("index_type", dc.all_index_types)  # , "BIN_FLAT"
	def test_task_2(self, index_type, data_size):
		"""
        before reinstall: create collection, insert data and create index,load and search
        after reinstall: get collection, load, search, insert data, create index, load, and search
		"""
		name = "task_2_" + index_type
		insert_data = True
		is_binary = True if "BIN" in index_type else False
		is_flush = False
		# create collection and insert data
		collection_w = self.init_collection_general(insert_data=insert_data, is_binary=is_binary, nb=data_size,
													is_flush=is_flush, name=name, active_trace=True)[0]
		vectors_to_search = cf.gen_vectors(default_nb, default_dim)
		default_search_field = ct.default_float_vec_field_name
		if is_binary:
			_, vectors_to_search = cf.gen_binary_vectors(default_nb, default_dim)
			default_search_field = ct.default_binary_vec_field_name
		# create index
		default_index = gen_index_param(index_type)
		collection_w.create_index(default_search_field, default_index)
		# load
		collection_w.load()
		# search
		search_params = gen_search_param(index_type)[0]
		collection_w.search(vectors_to_search[:default_nq], default_search_field,
							search_params, default_limit,
							default_search_exp,
							check_task=CheckTasks.check_search_results,
							check_items={"nq": default_nq,
										 "limit": default_limit})
		# query
		output_fields = [ct.default_int64_field_name]
		collection_w.query(default_term_expr, output_fields=output_fields,
						   check_task=CheckTasks.check_query_not_empty)

























