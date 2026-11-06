# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared REST schemas and exact vectors for nullable Struct children."""

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
