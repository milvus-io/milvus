import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

REST_VECTOR_DIM = 8
STRUCT_CAPACITY = 4
SUB_FIELDS = ("lang", "score", "emb", "tags")


def gen_vector(seed: int) -> list[float]:
    # halves are exact in float32, so vectors read back compare equal
    return [float(seed) + j * 0.5 for j in range(REST_VECTOR_DIM)]


def clips_struct_field(nullable: bool = True) -> dict:
    return {
        "fieldName": "clips",
        "nullable": nullable,
        "typeParams": {"max_capacity": str(STRUCT_CAPACITY)},
        "fields": [
            {
                "fieldName": "lang",
                "dataType": "Array",
                "elementDataType": "VarChar",
                "elementNullable": True,
                "elementTypeParams": {"max_length": "16"},
            },
            {
                "fieldName": "score",
                "dataType": "Array",
                "elementDataType": "Int64",
            },
            {
                "fieldName": "emb",
                "dataType": "ArrayOfVector",
                "elementDataType": "FloatVector",
                "elementNullable": True,
                "elementTypeParams": {"dim": str(REST_VECTOR_DIM)},
            },
            {
                # Array<VarChar>: an inner array may be null, and so may a leaf
                "fieldName": "tags",
                "dataType": "Array",
                "elementDataType": "Array",
                "elementNullable": True,
                "typeSchema": {
                    "arrayElement": {
                        "typeParams": {"max_capacity": "3"},
                        "arrayElement": {
                            "leafType": "VarChar",
                            "nullable": True,
                            "typeParams": {"max_length": "16"},
                        },
                    }
                },
            },
        ],
    }


def normalize_clips(clips, sub_fields=SUB_FIELDS):
    """What a row's clips read back as: an omitted element-nullable sub-field is a null element."""
    if clips is None:
        return None
    return [{name: element.get(name) for name in sub_fields} for element in clips]


def gen_rows() -> list[dict]:
    return [
        {
            "id": 1,
            "normal_vector": gen_vector(1),
            "clips": [
                {"lang": "en", "score": 10, "emb": gen_vector(10), "tags": ["a", None]},
                {"lang": None, "score": 11, "emb": None, "tags": None},
                {"score": 12, "tags": []},
            ],
        },
        {
            "id": 2,
            "normal_vector": gen_vector(2),
            "clips": [
                {"lang": "fr", "score": 20, "emb": gen_vector(20), "tags": [None, "b", "c"]},
                {"lang": "en", "score": 21, "emb": gen_vector(21)},
            ],
        },
        {"id": 3, "normal_vector": gen_vector(3), "clips": None},
        {"id": 4, "normal_vector": gen_vector(4), "clips": []},
        {
            "id": 5,
            "normal_vector": gen_vector(5),
            # every element-nullable sub-field omitted in every element
            "clips": [{"score": 50}, {"score": 51}],
        },
    ]


@pytest.mark.tags(CaseLabel.L1)
class TestRestfulStructArrayElementNull(TestBase):
    dim = REST_VECTOR_DIM

    def _create_collection(self, name: str, struct_field: dict | None = None):
        payload = {
            "collectionName": name,
            "schema": {
                "autoID": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "normal_vector",
                        "dataType": "FloatVector",
                        "elementTypeParams": {"dim": str(self.dim)},
                    },
                ],
                "structFields": [struct_field or clips_struct_field()],
            },
            "indexParams": [
                {
                    "fieldName": "normal_vector",
                    "indexName": "normal_vector",
                    "metricType": "L2",
                    "indexType": "HNSW",
                    "params": {"M": 16, "efConstruction": 200},
                },
                {
                    "fieldName": "clips[emb]",
                    "indexName": "clips_emb",
                    "metricType": "L2",
                    "indexType": "HNSW",
                    "params": {"M": 16, "efConstruction": 200},
                },
            ],
            "params": {"consistencyLevel": "Strong"},
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(name, timeout=60)

    def _insert(self, name: str, rows: list[dict]):
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows)

    def _query_clips(self, name: str, output_fields=None, sub_fields=SUB_FIELDS) -> dict:
        rsp = self.vector_client.vector_query(
            {
                "collectionName": name,
                "filter": "id >= 0",
                "outputFields": output_fields or ["id", "clips"],
                "limit": 100,
            }
        )
        assert rsp["code"] == 0, rsp
        return {row["id"]: normalize_clips(row["clips"], sub_fields) for row in rsp["data"]}

    def test_rest_v2_element_nullable_create_describe(self):
        """
        target: test REST v2 create/describe of element-nullable struct sub-fields
        method: create a struct with elementNullable scalar, vector and Array<VarChar> sub-fields
        expected: describe reports elementNullable and typeSchema, and only on the sub-fields that set them
        """
        name = gen_collection_name()
        self.name = name
        self._create_collection(name)

        rsp = self.collection_client.collection_describe(name)
        assert rsp["code"] == 0, rsp
        clips = next(field for field in rsp["data"]["structFields"] if field["name"] == "clips")
        assert clips["nullable"] is True
        subs = {sub["name"]: sub for sub in clips["fields"]}
        assert set(subs) == set(SUB_FIELDS)
        assert subs["lang"].get("elementNullable") is True
        assert subs["emb"].get("elementNullable") is True
        assert subs["tags"].get("elementNullable") is True
        assert "elementNullable" not in subs["score"]
        for name_ in ("lang", "score", "emb"):
            assert "typeSchema" not in subs[name_]

        tags = subs["tags"]
        assert tags["elementType"] == "Array"
        root = tags["typeSchema"]
        assert root["typeParams"]["max_capacity"] == str(STRUCT_CAPACITY)
        inner = root["arrayElement"]
        assert inner["nullable"] is True
        assert inner["typeParams"]["max_capacity"] == "3"
        leaf = inner["arrayElement"]
        assert leaf["leafType"] == "VarChar"
        assert leaf["nullable"] is True
        assert leaf["typeParams"]["max_length"] == "16"

    def test_rest_v2_element_null_insert_query_search(self):
        """
        target: test REST v2 element-level null round trip
        method: insert JSON null elements, omitted element-nullable sub-fields, null inner arrays and null
            leaves together with row-level null, then query (growing and sealed), query a single sub-field,
            search, and upsert
        expected: every element reads back as written; an omitted element-nullable sub-field reads as null
        """
        name = gen_collection_name()
        self.name = name
        self._create_collection(name)
        rows = gen_rows()
        expected = {row["id"]: normalize_clips(row["clips"]) for row in rows}
        self._insert(name, rows)

        assert self._query_clips(name) == expected

        rsp = self.collection_client.flush(name)
        assert rsp["code"] == 0, rsp
        assert self._query_clips(name) == expected

        # one sub-field output keeps the struct shape with only that sub-field
        lang_only = self._query_clips(name, ["id", "clips[lang]"], sub_fields=("lang",))
        assert lang_only == {row["id"]: normalize_clips(row["clips"], ("lang",)) for row in rows}

        rsp = self.vector_client.vector_search(
            {
                "collectionName": name,
                "data": [gen_vector(2)],
                "annsField": "normal_vector",
                "limit": len(rows),
                "outputFields": ["id", "clips"],
                "searchParams": {"metricType": "L2", "params": {"ef": 64}},
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"][0]["id"] == 2
        for hit in rsp["data"]:
            assert normalize_clips(hit["clips"]) == expected[hit["id"]]
            # not an element-level search: hits keep their shape
            assert "offset" not in hit

        upserted = {
            "id": 1,
            "normal_vector": gen_vector(1),
            "clips": [{"lang": None, "score": 13, "emb": gen_vector(13), "tags": [None]}],
        }
        rsp = self.vector_client.vector_upsert({"collectionName": name, "data": [upserted]})
        assert rsp["code"] == 0, rsp
        expected[1] = normalize_clips(upserted["clips"])
        assert self._query_clips(name) == expected

    def test_rest_v2_element_null_insert_rejected(self):
        """
        target: test REST v2 rejects element values the schema does not allow
        method: insert a null or a missing value for a sub-field that is not element nullable, an unknown
            sub-field key, and an inner array that is not an array
        expected: each insert is rejected and nothing is written
        """
        name = gen_collection_name()
        self.name = name
        self._create_collection(name)

        bad_clips = [
            [{"lang": "en", "score": None}],
            [{"lang": "en"}],
            [{"lang": "en", "score": 1, "unknown": 1}],
            [{"lang": "en", "score": 1, "tags": "a"}],
            [{"lang": "en", "score": 1, "emb": [0.5] * (self.dim - 1) + [None]}],
        ]
        for clips in bad_clips:
            rsp = self.vector_client.vector_insert(
                {
                    "collectionName": name,
                    "data": [{"id": 1, "normal_vector": gen_vector(1), "clips": clips}],
                }
            )
            assert rsp["code"] != 0, (clips, rsp)

        assert self._query_clips(name) == {}

    def test_rest_v2_element_filter_query_offset(self):
        """
        target: test REST v2 element-level query output
        method: query with element_filter on an element-nullable sub-field, including IS NULL
        expected: one row per matching entity, each with "offset" listing its matched elements
        """
        name = gen_collection_name()
        self.name = name
        self._create_collection(name)
        rows = gen_rows()
        expected = {row["id"]: normalize_clips(row["clips"]) for row in rows}
        self._insert(name, rows)
        rsp = self.collection_client.flush(name)
        assert rsp["code"] == 0, rsp

        def matched(predicate):
            return {
                row_id: [i for i, element in enumerate(clips) if predicate(element)]
                for row_id, clips in expected.items()
                if clips and any(predicate(element) for element in clips)
            }

        for expr, predicate in [
            ('element_filter(clips, $[lang] == "en")', lambda e: e["lang"] == "en"),
            ("element_filter(clips, $[lang] IS NULL)", lambda e: e["lang"] is None),
            ("element_filter(clips, $[score] > 11)", lambda e: e["score"] > 11),
        ]:
            rsp = self.vector_client.vector_query(
                {
                    "collectionName": name,
                    "filter": expr,
                    "outputFields": ["id", "clips"],
                    "limit": 100,
                }
            )
            assert rsp["code"] == 0, (expr, rsp)
            want = matched(predicate)
            # rows are not expanded per element
            assert sorted(row["id"] for row in rsp["data"]) == sorted(want), (expr, rsp)
            for row in rsp["data"]:
                assert sorted(row["offset"]) == want[row["id"]], (expr, row)
                assert normalize_clips(row["clips"]) == expected[row["id"]]

        # a query that is not element-level keeps its shape
        rsp = self.vector_client.vector_query(
            {"collectionName": name, "filter": "id >= 0", "outputFields": ["id"], "limit": 100}
        )
        assert rsp["code"] == 0, rsp
        assert all("offset" not in row for row in rsp["data"])

    def test_rest_v2_element_level_search_offset(self):
        """
        target: test REST v2 element-level search on an element-nullable vector sub-field
        method: search clips[emb] with a plain vector, then hybrid search it with normal_vector
        expected: every hit carries "offset" pointing at a non-null embedding of that entity; the nearest
            hit is the element the query vector was copied from
        """
        name = gen_collection_name()
        self.name = name
        self._create_collection(name)
        rows = gen_rows()
        expected = {row["id"]: normalize_clips(row["clips"]) for row in rows}
        self._insert(name, rows)
        rsp = self.collection_client.flush(name)
        assert rsp["code"] == 0, rsp

        rsp = self.vector_client.vector_search(
            {
                "collectionName": name,
                "data": [gen_vector(21)],
                "annsField": "clips[emb]",
                "limit": 3,
                "outputFields": ["id", "clips"],
            }
        )
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) > 0, rsp
        top = rsp["data"][0]
        assert top["id"] == 2 and top["offset"] == 1, top
        for hit in rsp["data"]:
            clips = expected[hit["id"]]
            assert isinstance(hit["offset"], int), hit
            assert 0 <= hit["offset"] < len(clips), hit
            assert clips[hit["offset"]]["emb"] is not None, hit

        rsp = self.vector_client.vector_advanced_search(
            {
                "collectionName": name,
                "search": [
                    {"data": [gen_vector(21)], "annsField": "clips[emb]", "limit": 3},
                    {"data": [gen_vector(2)], "annsField": "normal_vector", "limit": 3},
                ],
                "rerank": {"strategy": "rrf", "params": {"k": 10}},
                "limit": 3,
                "outputFields": ["id"],
            }
        )
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) > 0, rsp
        for hit in rsp["data"]:
            # a hybrid result may collapse element hits; when it keeps them
            # the offset is an element index
            if "offset" in hit:
                assert isinstance(hit["offset"], int), hit

    def test_rest_v2_add_drop_struct_sub_field(self):
        """
        target: test REST v2 add and drop of a struct sub-field through fields/add and fields/drop
        method: add clips[note] as an element-nullable VarChar, read old and new rows, then drop it
        expected: old rows read note as null in every element; new rows read it as written; after the drop
            note is gone from describe and from writes
        """
        name = gen_collection_name()
        self.name = name
        self._create_collection(name)
        rows = gen_rows()
        self._insert(name, rows)
        rsp = self.collection_client.flush(name)
        assert rsp["code"] == 0, rsp

        rsp = self.collection_client.add_field(
            name,
            {
                "fieldName": "clips[note]",
                "dataType": "Array",
                "elementDataType": "VarChar",
                "elementNullable": True,
                "elementTypeParams": {"max_length": "32"},
            },
        )
        assert rsp["code"] == 0, rsp

        rsp = self.collection_client.collection_describe(name)
        assert rsp["code"] == 0, rsp
        clips = next(field for field in rsp["data"]["structFields"] if field["name"] == "clips")
        note = next(sub for sub in clips["fields"] if sub["name"] == "note")
        assert note.get("elementNullable") is True
        assert "clips[note]" not in {field["name"] for field in rsp["data"]["fields"]}

        with_note = SUB_FIELDS + ("note",)
        expected = {row["id"]: normalize_clips(row["clips"], with_note) for row in rows}
        new_row = {
            "id": 6,
            "normal_vector": gen_vector(6),
            "clips": [
                {"lang": "de", "score": 60, "note": "hello"},
                {"lang": "en", "score": 61, "emb": gen_vector(61), "note": None},
            ],
        }
        self._insert(name, [new_row])
        expected[6] = normalize_clips(new_row["clips"], with_note)
        assert self._query_clips(name, sub_fields=with_note) == expected

        rsp = self.collection_client.drop_field(name, field_name="clips[note]")
        assert rsp["code"] == 0, rsp
        rsp = self.collection_client.collection_describe(name)
        assert rsp["code"] == 0, rsp
        clips = next(field for field in rsp["data"]["structFields"] if field["name"] == "clips")
        assert "note" not in {sub["name"] for sub in clips["fields"]}

        expected = {row_id: normalize_clips(value) for row_id, value in expected.items()}
        assert self._query_clips(name) == expected

        # a write that still carries the dropped sub-field is rejected
        rsp = self.vector_client.vector_insert(
            {
                "collectionName": name,
                "data": [{"id": 7, "normal_vector": gen_vector(7), "clips": [{"score": 70, "note": "x"}]}],
            }
        )
        assert rsp["code"] != 0, rsp

    def test_rest_v2_add_struct_sub_field_rejected(self):
        """
        target: test REST v2 fields/add rejects a malformed or non-element-nullable struct sub-field
        method: add clips[x][y], an empty sub-field name, and a sub-field without elementNullable
        expected: every request is rejected and the struct keeps its sub-fields
        """
        name = gen_collection_name()
        self.name = name
        self._create_collection(name)

        for field_name, element_nullable in [
            ("clips[a][b]", True),
            ("clips[]", True),
            ("clips[plain]", False),
        ]:
            rsp = self.collection_client.add_field(
                name,
                {
                    "fieldName": field_name,
                    "dataType": "Array",
                    "elementDataType": "Int64",
                    "elementNullable": element_nullable,
                },
            )
            assert rsp["code"] != 0, (field_name, rsp)

        rsp = self.collection_client.collection_describe(name)
        assert rsp["code"] == 0, rsp
        clips = next(field for field in rsp["data"]["structFields"] if field["name"] == "clips")
        assert {sub["name"] for sub in clips["fields"]} == set(SUB_FIELDS)

    def test_rest_v2_add_struct_field_with_element_nullable_sub_fields(self):
        """
        target: test REST v2 struct_fields/add with element-nullable sub-fields
        method: add a nullable struct whose sub-fields are an element-nullable scalar and an Array<Int64>
        expected: describe reports the new struct's elementNullable and typeSchema; rows written later read back
        """
        name = gen_collection_name()
        self.name = name
        self._create_collection(name)

        rsp = self.collection_client.add_struct_field(
            name,
            {
                "fieldName": "notes",
                "dataType": "ArrayOfStruct",
                "nullable": True,
                "typeParams": {"max_capacity": "4"},
                "fields": [
                    {
                        "fieldName": "text",
                        "dataType": "Array",
                        "elementDataType": "VarChar",
                        "elementNullable": True,
                        "elementTypeParams": {"max_length": "32"},
                    },
                    {
                        "fieldName": "marks",
                        "dataType": "Array",
                        "elementDataType": "Array",
                        "typeSchema": {
                            "arrayElement": {
                                "typeParams": {"max_capacity": "2"},
                                "arrayElement": {"leafType": "Int64"},
                            }
                        },
                    },
                ],
            },
        )
        assert rsp["code"] == 0, rsp

        rsp = self.collection_client.collection_describe(name)
        assert rsp["code"] == 0, rsp
        notes = next(field for field in rsp["data"]["structFields"] if field["name"] == "notes")
        subs = {sub["name"]: sub for sub in notes["fields"]}
        assert subs["text"].get("elementNullable") is True
        assert "elementNullable" not in subs["marks"]
        assert subs["marks"]["typeSchema"]["arrayElement"]["arrayElement"]["leafType"] == "Int64"

        row = {
            "id": 1,
            "normal_vector": gen_vector(1),
            "clips": [{"score": 1}],
            "notes": [{"text": None, "marks": [1, 2]}, {"marks": []}],
        }
        self._insert(name, [row])
        rsp = self.vector_client.vector_query(
            {"collectionName": name, "filter": "id >= 0", "outputFields": ["id", "notes"], "limit": 10}
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"][0]["notes"] == [{"text": None, "marks": [1, 2]}, {"text": None, "marks": []}]
