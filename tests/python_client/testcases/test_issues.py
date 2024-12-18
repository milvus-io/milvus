from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
import random
import pytest


class TestIssues(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("par_key_field", [ct.default_int64_field_name])
    @pytest.mark.parametrize("use_upsert", [True, False])
    def test_issue_30607(self, par_key_field, use_upsert):
        """
        Method：
        1. create a collection with partition key on collection schema with customized num_partitions
        2. randomly check 200 entities
        2. verify partition key values are hashed into correct partitions
        """
        self._connect()
        pk_field = cf.gen_string_field(name='pk', is_primary=True)
        int64_field = cf.gen_int64_field()
        string_field = cf.gen_string_field()
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, int64_field, string_field, vector_field],
                                          auto_id=False, partition_key_field=par_key_field)
        c_name = cf.gen_unique_str("par_key")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema, num_partitions=9)

        # insert
        nb = 500
        string_prefix = cf.gen_str_by_length(length=6)
        entities_per_parkey = 20
        for n in range(entities_per_parkey):
            pk_values = [str(i) for i in range(n * nb, (n+1)*nb)]
            int64_values = [i for i in range(0, nb)]
            string_values = [string_prefix + str(i) for i in range(0, nb)]
            float_vec_values = gen_vectors(nb, ct.default_dim)
            data = [pk_values, int64_values, string_values, float_vec_values]
            if use_upsert:
                collection_w.upsert(data)
            else:
                collection_w.insert(data)

        # flush
        collection_w.flush()
        num_entities = collection_w.num_entities
        # build index
        collection_w.create_index(field_name=vector_field.name, index_params=ct.default_index)

        for index_on_par_key_field in [False, True]:
            collection_w.release()
            if index_on_par_key_field:
                collection_w.create_index(field_name=par_key_field, index_params={})
            # load
            collection_w.load()

            # verify the partition key values are bashed correctly
            seeds = 200
            rand_ids = random.sample(range(0, num_entities), seeds)
            rand_ids = [str(rand_ids[i]) for i in range(len(rand_ids))]
            res, _ = collection_w.query(expr=f"pk in {rand_ids}", output_fields=["pk", par_key_field])
            # verify every the random id exists
            assert len(res) == len(rand_ids)

            dirty_count = 0
            for i in range(len(res)):
                pk = res[i].get("pk")
                parkey_value = res[i].get(par_key_field)
                res_parkey, _ = collection_w.query(expr=f"{par_key_field}=={parkey_value} and pk=='{pk}'",
                                                   output_fields=["pk", par_key_field])
                if len(res_parkey) != 1:
                    log.info(f"dirty data found: pk {pk} with parkey {parkey_value}")
                    dirty_count += 1
                    assert dirty_count == 0
            log.info(f"check randomly {seeds}/{num_entities}, dirty count={dirty_count}")

    @pytest.mark.tags(CaseLabel.L2)
    def test_issue_32294(self):
        """
        Method：
        1. create a collection with partition key on collection schema with customized num_partitions
        2. randomly check 200 entities
        2. verify partition key values are hashed into correct partitions
        """
        self._connect()
        pk_field = cf.gen_int64_field(name='pk', is_primary=True)
        string_field = cf.gen_string_field(name="metadata")
        vector_field = cf.gen_float_vec_field()
        schema = cf.gen_collection_schema(fields=[pk_field, string_field, vector_field], auto_id=True)
        collection_w = self.init_collection_wrap(schema=schema)

        # insert
        nb = 500
        string_values = [str(i) for i in range(0, nb)]
        float_vec_values = gen_vectors(nb, ct.default_dim)
        string_values[0] = ('{\n'
                            '"Header 1": "Foo1?", \n'
                            '"document_category": "acme", \n'
                            '"type": "passage"\n'
                            '}')
        string_values[1] = '{"Header 1": "Foo1?", "document_category": "acme", "type": "passage"}'
        data = [string_values, float_vec_values]
        collection_w.insert(data)
        collection_w.create_index(field_name=ct.default_float_vec_field_name, index_params=ct.default_index)
        collection_w.load()

        expr = "metadata like '%passage%'"
        collection_w.search(float_vec_values[-2:], ct.default_float_vec_field_name, {},
                            ct.default_limit, expr, output_fields=["metadata"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 2,
                                         "limit": 2})
