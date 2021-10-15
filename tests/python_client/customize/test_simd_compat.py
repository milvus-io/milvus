import pytest

from pymilvus import connections
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from common.common_type import CaseLabel


supported_simd_types = ["sse4_2", "avx", "avx2", "avx512"]


class TestSimdCompatibility:

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('simd', ["sse4_2", "avx", "avx2", "avx512"])
    def test_simd_compat(self, simd):
        """
        steps
        1. set up milvus with customized simd configured
        2. verify milvus is working well
        3. verify milvus is running against the configured simd
        4. clear the env
        """
        pass




