import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import DataType

EVICTABLE_KEY = "evictable"
SCALAR_FIELD_EVICTABLE_KEY = "evictable.scalarField"
SCALAR_INDEX_EVICTABLE_KEY = "evictable.scalarIndex"
VECTOR_FIELD_EVICTABLE_KEY = "evictable.vectorField"
VECTOR_INDEX_EVICTABLE_KEY = "evictable.vectorIndex"
COLLECTION_EVICTABLE_KEYS = (
    SCALAR_FIELD_EVICTABLE_KEY,
    SCALAR_INDEX_EVICTABLE_KEY,
    VECTOR_FIELD_EVICTABLE_KEY,
    VECTOR_INDEX_EVICTABLE_KEY,
)


class TestMilvusClientEvictable(TestMilvusClientV2Base):
    """End-to-end coverage for tiered-storage evictable metadata properties."""

    @staticmethod
    def _add_schema_fields(schema, dim=16):
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("scalar", DataType.INT64)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dim)

    def _create_evictable_collection(self, client, collection_name, properties=None, index_evictable=None):
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        self._add_schema_fields(schema)

        index_params = self.prepare_index_params(client)[0]
        index_kwargs = {
            "index_type": "HNSW",
            "metric_type": "L2",
            "params": {"M": 16, "efConstruction": 100},
        }
        if index_evictable is not None:
            index_kwargs["params"][EVICTABLE_KEY] = index_evictable
        index_params.add_index(field_name="vector", **index_kwargs)
        create_kwargs = {"consistency_level": "Strong"}
        if properties is not None:
            create_kwargs["properties"] = properties
        self.create_collection(client, collection_name, schema=schema, index_params=index_params, **create_kwargs)

    @pytest.mark.tags(CaseLabel.L0)
    def test_evictable_create_describe_load_search(self):
        """Collection, index, and field keys survive the load/query path."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        collection_properties = {
            SCALAR_FIELD_EVICTABLE_KEY: False,
            SCALAR_INDEX_EVICTABLE_KEY: False,
            VECTOR_FIELD_EVICTABLE_KEY: True,
            VECTOR_INDEX_EVICTABLE_KEY: True,
        }
        self._create_evictable_collection(
            client,
            collection_name,
            properties=collection_properties,
            index_evictable=False,
        )

        description = self.describe_collection(client, collection_name)[0]
        properties = description.get("properties", {})
        for key, value in collection_properties.items():
            assert properties.get(key) == str(value)

        index_description = self.describe_index(client, collection_name, "vector")[0]
        assert index_description.get(EVICTABLE_KEY) == "False"

        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="vector",
            field_params={EVICTABLE_KEY: False},
        )
        description = self.describe_collection(client, collection_name)[0]
        vector_field = next(field for field in description["fields"] if field["name"] == "vector")
        assert vector_field["params"].get(EVICTABLE_KEY) == "False"

        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=description)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        search_res = self.search(
            client,
            collection_name,
            cf.gen_vectors(1, 16),
            limit=5,
            output_fields=["scalar"],
        )[0]
        assert len(search_res[0]) == 5
        query_res = self.query(
            client,
            collection_name,
            filter="id >= 0",
            limit=5,
            output_fields=["id", "scalar"],
        )[0]
        assert len(query_res) == 5

    @pytest.mark.tags(CaseLabel.L1)
    def test_evictable_alter_drop_lifecycle(self):
        """Collection and index defaults can be altered and dropped while released."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create_evictable_collection(client, collection_name)
        self.release_collection(client, collection_name)

        self.alter_collection_properties(
            client,
            collection_name,
            properties={
                SCALAR_FIELD_EVICTABLE_KEY: False,
                SCALAR_INDEX_EVICTABLE_KEY: False,
                VECTOR_FIELD_EVICTABLE_KEY: False,
                VECTOR_INDEX_EVICTABLE_KEY: False,
            },
        )
        description = self.describe_collection(client, collection_name)[0]
        properties = description.get("properties", {})
        assert properties[SCALAR_FIELD_EVICTABLE_KEY] == "False"
        assert properties[VECTOR_INDEX_EVICTABLE_KEY] == "False"

        self.alter_collection_field(
            client,
            collection_name,
            field_name="vector",
            field_params={EVICTABLE_KEY: True},
        )
        description = self.describe_collection(client, collection_name)[0]
        vector_field = next(field for field in description["fields"] if field["name"] == "vector")
        assert vector_field["params"].get(EVICTABLE_KEY) == "True"

        self.alter_index_properties(
            client,
            collection_name,
            index_name="vector",
            properties={EVICTABLE_KEY: True},
        )
        index_description = self.describe_index(client, collection_name, "vector")[0]
        assert index_description.get(EVICTABLE_KEY) == "True"

        self.drop_collection_properties(
            client,
            collection_name,
            property_keys=[
                SCALAR_FIELD_EVICTABLE_KEY,
                SCALAR_INDEX_EVICTABLE_KEY,
                VECTOR_FIELD_EVICTABLE_KEY,
                VECTOR_INDEX_EVICTABLE_KEY,
            ],
        )
        description = self.describe_collection(client, collection_name)[0]
        properties = description.get("properties", {})
        for key in COLLECTION_EVICTABLE_KEYS:
            assert key not in properties

        self.drop_index_properties(client, collection_name, "vector", property_keys=[EVICTABLE_KEY])
        index_description = self.describe_index(client, collection_name, "vector")[0]
        assert EVICTABLE_KEY not in index_description

        description = self.describe_collection(client, collection_name)[0]
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=description)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        search_res = self.search(client, collection_name, cf.gen_vectors(1, 16), limit=5)[0]
        assert len(search_res[0]) == 5

    @pytest.mark.tags(CaseLabel.L1)
    def test_evictable_invalid_and_loaded_state(self):
        """Invalid values and loaded-collection mutations are rejected."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self._create_evictable_collection(client, collection_name)
        self.release_collection(client, collection_name)

        self.alter_collection_properties(
            client,
            collection_name,
            properties={SCALAR_FIELD_EVICTABLE_KEY: "not-bool"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid evictable"},
        )
        self.alter_collection_properties(
            client,
            collection_name,
            properties={EVICTABLE_KEY: False},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "only allowed at field level"},
        )
        self.alter_collection_field(
            client,
            collection_name,
            field_name="vector",
            field_params={EVICTABLE_KEY: "not-bool"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid evictable"},
        )
        self.alter_index_properties(
            client,
            collection_name,
            index_name="vector",
            properties={EVICTABLE_KEY: "not-bool"},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "invalid evictable"},
        )

        self.load_collection(client, collection_name)
        self.alter_collection_properties(
            client,
            collection_name,
            properties={VECTOR_FIELD_EVICTABLE_KEY: False},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 104, ct.err_msg: "can not alter evictable properties if collection loaded"},
        )
        self.alter_collection_field(
            client,
            collection_name,
            field_name="vector",
            field_params={EVICTABLE_KEY: False},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 104, ct.err_msg: "can not alter evictable if collection loaded"},
        )
        self.alter_index_properties(
            client,
            collection_name,
            index_name="vector",
            properties={EVICTABLE_KEY: False},
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 104, ct.err_msg: "can't alter index on loaded collection"},
        )
