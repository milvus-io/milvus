import json
import os
import time
from collections import defaultdict

import pytest
from base.client_base import TestcaseBase
from chaos import constants
from chaos.recovery_continuation import is_recovery_collection
from common.common_type import CaseLabel
from deploy.common import get_chaos_test_collections
from pymilvus import Collection
from utils.util_log import test_log as log


class TestGetCollections(TestcaseBase):
    """ Test case of getting all collections """

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_collections_by_prefix(self,):
        self._connect()
        all_collections = self.utility_wrap.list_collections()[0]
        recovery_collections = [name for name in all_collections if is_recovery_collection(name)]
        all_collections = [c_name for c_name in all_collections if c_name.startswith("Checker")]
        selected_collections_map = {}
        for c_name in all_collections:
            if Collection(name=c_name).num_entities < constants.ENTITIES_FOR_SEARCH:
                continue
            prefix = c_name.split("_")[0]
            if prefix not in selected_collections_map:
                selected_collections_map[prefix] = [c_name]
            else:
                if len(selected_collections_map[prefix]) <= 5:
                    selected_collections_map[prefix].append(c_name)

        selected_collections = []
        for value in selected_collections_map.values():
            selected_collections.extend(value)
        # Never call num_entities on these collections: it can force a flush.
        # Keep every dedicated ledger owner, including small growing workloads.
        selected_collections.extend(recovery_collections)
        log.info(f"find {len(selected_collections)} collections:")
        log.info(selected_collections)
        data = {
            "all": selected_collections,
        }
        os.makedirs("/tmp/ci_logs", exist_ok=True)
        with open("/tmp/ci_logs/chaos_test_all_collections.json", "w") as f:
            json.dump(data, f)
        log.info(f"write {len(selected_collections)} collections to /tmp/ci_logs/chaos_test_all_collections.json")
        collections_in_json = get_chaos_test_collections()
        assert len(selected_collections) == len(collections_in_json)
