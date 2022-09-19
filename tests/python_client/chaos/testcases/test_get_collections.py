import time
import json
from collections import defaultdict
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from deploy.common import get_collections
from common.common_type import CaseLabel
from utils.util_log import test_log as log


class TestGetCollections(TestcaseBase):
    """ Test case of getting all collections """

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_collections_by_prefix(self,):
        self._connect()
        all_collections = self.utility_wrap.list_collections()[0]
        all_collections = [c_name for c_name in all_collections if "Checker" in c_name]
        selected_collections_map = {}
        for c_name in all_collections:
            prefix = c_name.split("_")[0]
            if prefix not in selected_collections_map:
                selected_collections_map[prefix] = [c_name]
            else:
                if len(selected_collections_map[prefix]) <= 5:
                    selected_collections_map[prefix].append(c_name)

        selected_collections = []
        for value in selected_collections_map.values():
            selected_collections.extend(value)
        log.info(f"find {len(selected_collections)} collections:")
        log.info(selected_collections)
        data = {
            "all": selected_collections,
        }
        with open("/tmp/ci_logs/all_collections.json", "w") as f:
            f.write(json.dumps(data))
        log.info(f"write {len(selected_collections)} collections to /tmp/ci_logs/all_collections.json")
        collections_in_json = get_collections()
        assert len(selected_collections) == len(collections_in_json)
