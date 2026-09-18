import random

import pytest
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "nprobe_validation"
ivf_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
field = ct.default_float_vec_field_name
nprobe_zero = {"metric_type": "L2", "params": {"nprobe": 0}}
nprobe_valid = {"metric_type": "L2", "params": {"nprobe": 10}}
err_items = {
    ct.err_code: 0,
    ct.err_msg: "Out of range in json: param 'nprobe' (0)",
}


class TestNprobeValidation(TestcaseBase):
    """Regression for #47729: nprobe=0 must be rejected on every search path.

    ValidateVectorSearchParams runs at plan creation (plan_c.cpp), before any
    segment dispatch, so it covers growing / sealed-unindexed / sealed-with-
    binlog-index / sealed-with-index paths uniformly — the rejection happens at
    the plan, which every search path shares.

    The sealed-with-binlog-index (interimIndex.enableIndex=true) path cannot be
    exercised here: milvus's e2e framework has no API to toggle that
    queryNode config at runtime, so interimIndex has never had an e2e test in
    milvus. It is covered by construction because plan creation precedes the
    binlog-index dispatch (ChunkedSegmentSealedImpl branch 1).
    """

    def _vectors(self):
        return [[random.random() for _ in range(ct.default_dim)] for _ in range(ct.default_nq)]

    @pytest.mark.tags(CaseLabel.L1)
    def test_nprobe_zero_rejected_sealed_with_index(self):
        """nprobe=0 rejected on a sealed segment with the IVF index loaded."""
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        collection_w.insert(cf.gen_default_dataframe_data(nb=ct.default_nb))
        collection_w.flush()
        collection_w.create_index(field, ivf_index)
        collection_w.load()
        collection_w.search(
            self._vectors(), field, nprobe_zero, ct.default_limit, check_task=CheckTasks.err_res, check_items=err_items
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_nprobe_zero_rejected_growing(self):
        """nprobe=0 rejected on a growing segment (insert without flush)."""
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        collection_w.create_index(field, ivf_index)
        collection_w.insert(cf.gen_default_dataframe_data(nb=ct.default_nb))
        collection_w.load()
        collection_w.search(
            self._vectors(), field, nprobe_zero, ct.default_limit, check_task=CheckTasks.err_res, check_items=err_items
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_nprobe_valid_accepted(self):
        """Sanity: a valid nprobe still searches successfully."""
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        collection_w.insert(cf.gen_default_dataframe_data(nb=ct.default_nb))
        collection_w.flush()
        collection_w.create_index(field, ivf_index)
        collection_w.load()
        collection_w.search(
            self._vectors(),
            field,
            nprobe_valid,
            ct.default_limit,
            check_task=CheckTasks.check_search_results,
            check_items={"nq": ct.default_nq, "limit": ct.default_limit},
        )
