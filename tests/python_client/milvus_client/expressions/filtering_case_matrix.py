REAL_INDEX_ROW_COUNT = 3000

SEGMENT_MODES_ACTIVE = ["sealed", "growing", "mixed"]
SEGMENT_MODES_PENDING = []
SEGMENT_MODES_SMOKE = SEGMENT_MODES_ACTIVE
SEGMENT_MODES_FULL = SEGMENT_MODES_ACTIVE


ORDER_SENSITIVE_EXPRESSIONS = [
    (
        "age > 10 and score <= 90",
        "score <= 90 and age > 10",
        [3, 4, 6, 7, 8, 9, 10, 11, 14],
    ),
    (
        'age > 10 and meta["group"] == "qa"',
        'meta["group"] == "qa" and age > 10',
        [2, 4],
    ),
    (
        'tag == "ops" or active == true',
        'active == true or tag == "ops"',
        [1, 2, 4, 5, 6, 14],
    ),
    (
        '(age > 10 and meta["rank"] in [1, 3]) or active == true',
        'active == true or (meta["rank"] in [1, 3] and age > 10)',
        [1, 2, 3, 4, 11, 13, 14],
    ),
]


EQUIVALENT_EXPRESSION_CASES = [
    (
        "json_or_vs_in",
        'meta["p"] == 1 or meta["p"] == 3 or meta["p"] == 5',
        'meta["p"] in [1, 3, 5]',
        [1, 3, 5],
    ),
    (
        "de_morgan_scalar_bool",
        "not (age <= 10 or active == false)",
        "age > 10 and active == true",
        [2, 4, 14],
    ),
    (
        "distributive_scalar_bool_string",
        '(age > 10 and active == true) or (age > 10 and tag == "ops")',
        'age > 10 and (active == true or tag == "ops")',
        [2, 4, 5, 6, 14],
    ),
]


BOOLEAN_FANOUT_EXPRESSIONS_GENERALIZED_L2 = [
    ("single_predicate", "age > 10", 1, [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]),
    ("and_2", "age > 10 and score <= 90", 2, [3, 4, 6, 7, 8, 9, 10, 11, 14]),
    ("and_3", "age > 10 and score <= 90 and active == true", 3, [4, 14]),
    (
        "and_5_cross_field",
        'age > 10 and score <= 90 and active == false and tag != "ops" and meta["rank"] >= 3',
        5,
        [3, 7, 8, 9, 10],
    ),
    (
        "mixed_and_or_depth_2",
        '(age > 10 and meta["rank"] in [1, 3]) or active == true',
        3,
        [1, 2, 3, 4, 11, 13, 14],
    ),
]


SAME_FIELD_OR_FANOUT_CASES = [
    ("or_2_same_field", 'meta["p"] == 1 or meta["p"] == 2', 2, [1, 2]),
    (
        "or_3_same_field",
        'meta["p"] == 1 or meta["p"] == 2 or meta["p"] == 3',
        3,
        [1, 2, 3],
    ),
    (
        "or_5_same_field",
        'meta["p"] == 1 or meta["p"] == 2 or meta["p"] == 3 or meta["p"] == 4 or meta["p"] == 5',
        5,
        [1, 2, 3, 4, 5],
    ),
    (
        "or_10_same_field",
        " or ".join([f'meta["p"] == {i}' for i in range(1, 11)]),
        10,
        list(range(1, 11)),
    ),
]

SAME_FIELD_OR_FANOUT_EXPRESSIONS_GENERALIZED_L2 = [case for case in SAME_FIELD_OR_FANOUT_CASES if case[2] in {2, 3, 5}]
BOOLEAN_FANOUT_EXPRESSIONS_L2 = [case for case in SAME_FIELD_OR_FANOUT_CASES if case[2] == 10]


JSON_MIXED_TYPE_OR_51568_CASES = [
    ("pure_int_in_5_control", 'meta["p"] in [1, 2, 3, 4, 5]', 5, [1, 2, 3, 4, 5]),
    ("mixed_numeric_in_5_control", 'meta["p"] in [1.0, 2, 3, 4, 5]', 5, [1, 2, 3, 4, 5]),
    (
        "float_int_or_4",
        '(meta["p"] == 1.0) or (meta["p"] == 2) or (meta["p"] == 3) or (meta["p"] == 4)',
        4,
        [1, 2, 3, 4],
    ),
    (
        "float_int_or_5",
        '(meta["p"] == 1.0) or (meta["p"] == 2) or (meta["p"] == 3) or (meta["p"] == 4) or (meta["p"] == 5)',
        5,
        [1, 2, 3, 4, 5],
    ),
    (
        "float_middle_int_or_5",
        '(meta["p"] == 1) or (meta["p"] == 2) or (meta["p"] == 3.0) or (meta["p"] == 4) or (meta["p"] == 5)',
        5,
        [1, 2, 3, 4, 5],
    ),
    (
        "float_last_int_or_5",
        '(meta["p"] == 1) or (meta["p"] == 2) or (meta["p"] == 3) or (meta["p"] == 4) or (meta["p"] == 5.0)',
        5,
        [1, 2, 3, 4, 5],
    ),
    (
        "float_int_or_alternating_6",
        '(meta["p"] == 1.0) or (meta["p"] == 2) or (meta["p"] == 3.0) or '
        '(meta["p"] == 4) or (meta["p"] == 5.0) or (meta["p"] == 6)',
        6,
        [1, 2, 3, 4, 5, 6],
    ),
    (
        "float_int_or_20",
        " or ".join(['(meta["p"] == 1.0)'] + [f'(meta["p"] == {i})' for i in range(2, 21)]),
        20,
        list(range(1, 21)),
    ),
    (
        "str_int_or_3",
        '(meta["p"] == "1") or (meta["p"] == 2) or (meta["p"] == 3)',
        3,
        [2, 3, REAL_INDEX_ROW_COUNT],
    ),
]


JSON_MIXED_TYPE_IN_51489_CASES = [
    ("int_string_in", 'meta["p"] in [1, "2"]'),
    ("string_int_in", 'meta["p"] in ["1", 2]'),
    ("int_unrelated_string_in", 'meta["p"] in [1, "missing"]'),
    ("json_array_subscript_mixed_in", 'meta["arr"][0] in [1, "2"]'),
]


JSON_BOOL_MIXED_IN_51567_CASES = [
    ("bool_int_in_true_one", 'meta["b"] in [true, 1]'),
    ("bool_int_in_false_one", 'meta["b"] in [false, 1]'),
    ("bool_string_in", 'meta["b"] in [true, "yes"]'),
    ("bool_int_string_in", 'meta["b"] in [true, 1, "true"]'),
]


JSON_BOOL_MIXED_OR_51567_CASES = [
    (
        "bool_int_or_bool_first_three",
        '(meta["b"] == true) or (meta["b"] == 1) or (meta["b"] == 0)',
        [1, 2, 5, 6],
    ),
    (
        "bool_int_or_int_first_three",
        '(meta["b"] == 1) or (meta["b"] == true) or (meta["b"] == 0)',
        [1, 2, 5, 6],
    ),
    (
        "bool_int_or_bool_last_three",
        '(meta["b"] == 1) or (meta["b"] == 0) or (meta["b"] == true)',
        [1, 2, 5, 6],
    ),
    (
        "bool_int_or_false_first_three",
        '(meta["b"] == false) or (meta["b"] == 1) or (meta["b"] == 0)',
        [3, 4, 5, 6],
    ),
    (
        "bool_int_or_false_last_three",
        '(meta["b"] == 1) or (meta["b"] == 0) or (meta["b"] == false)',
        [3, 4, 5, 6],
    ),
    (
        "bool_string_or_three",
        '(meta["b"] == true) or (meta["b"] == "yes") or (meta["b"] == "no")',
        [1, 2, 7, 8],
    ),
]


JSON_BOOL_MIXED_51567_CONTROL_CASES = [
    ("bool_only_in", 'meta["b"] in [true, false]', [1, 2, 3, 4]),
    ("int_only_in", 'meta["b"] in [0, 1]', [5, 6]),
    ("bool_int_or_two_branches", '(meta["b"] == true) or (meta["b"] == 1)', [1, 2, 6]),
]


EMPTY_LIST_TEMPLATE_51617_CASES = [
    ("scalar_in", "id <= 2 and id in {values}", "id <= 2 and id in []", []),
    ("scalar_not_in", "id <= 2 and id not in {values}", "id <= 2 and id not in []", [1, 2]),
    (
        "array_contains_any",
        "id <= 2 and array_contains_any(tags, {values})",
        "id <= 2 and array_contains_any(tags, [])",
        [],
    ),
    (
        "array_contains_all",
        "id <= 2 and array_contains_all(tags, {values})",
        "id <= 2 and array_contains_all(tags, [])",
        [1, 2],
    ),
    (
        "json_contains_any",
        'id <= 2 and json_contains_any(meta["tags"], {values})',
        'id <= 2 and json_contains_any(meta["tags"], [])',
        [],
    ),
    (
        "json_contains_all",
        'id <= 2 and json_contains_all(meta["tags"], {values})',
        'id <= 2 and json_contains_all(meta["tags"], [])',
        [1, 2],
    ),
]


BITWISE_CONTROL_CASES = [
    ("bit_and", "(flags & 1) == 1", [1, 3, 5, 7]),
    ("bit_or", "(flags | 1) == 1", [0, 1]),
    ("bit_xor", "(flags ^ 1) == 0", [1]),
]


BITWISE_PENDING_50964_CASES = [
    ("shift_left", "flags << 1 == 4", [2]),
    ("shift_right", "flags >> 2 == 1", [4, 5, 6, 7]),
    ("bit_not", "~flags == -1", [0]),
]


NUMERIC_SCALAR_FIELDS = ["i8", "i16", "i32", "i64", "f", "d"]
NUMERIC_SCALAR_FIELD_SHIFTS = {
    "i8": 1,
    "i16": 2,
    "i32": 3,
    "i64": 0,
    "f": 4,
    "d": 5,
}
NUMERIC_SCALAR_SENTINEL_VALUES = {
    11: {
        "i8": 127,
        "i16": 32767,
        "i32": 2147483647,
        "i64": 9223372036854775807,
        "f": 127.5,
        "d": 1000000000000.25,
    },
    12: {
        "i8": -128,
        "i16": -32768,
        "i32": -2147483648,
        "i64": -9223372036854775808,
        "f": -127.5,
        "d": -1000000000000.25,
    },
}


def numeric_scalar_value(field, row_id):
    if row_id in NUMERIC_SCALAR_SENTINEL_VALUES:
        return NUMERIC_SCALAR_SENTINEL_VALUES[row_id][field]
    value = ((row_id + NUMERIC_SCALAR_FIELD_SHIFTS[field] - 1) % 10) + 1
    return float(value) if field in {"f", "d"} else value


NUMERIC_SCALAR_FILTER_TEMPLATES = [
    ("eq_3", "{field} == 3", lambda value: value == 3),
    ("range_3_6", "{field} > 3 and {field} <= 6", lambda value: 3 < value <= 6),
    ("in_odd", "{field} in [1, 3, 5]", lambda value: value in {1, 3, 5}),
    ("not_in_odd", "{field} not in [1, 3, 5]", lambda value: value not in {1, 3, 5}),
]
SCALAR_FILTER_CASES = [
    (
        f"{field}_{case_name}",
        expr_template.format(field=field),
        [row_id for row_id in range(1, 13) if predicate(numeric_scalar_value(field, row_id))],
    )
    for field in NUMERIC_SCALAR_FIELDS
    for case_name, expr_template, predicate in NUMERIC_SCALAR_FILTER_TEMPLATES
]
SCALAR_FILTER_CASES.extend(
    [
        ("varchar_like_prefix", 'name like "user_%"', list(range(1, 10))),
        ("varchar_in", 'name in ["user_1", "user_3"]', [1, 3]),
        ("bool_true", "active == true", [2, 4, 6, 8, 10, 11, 12]),
        ("bool_false", "active == false", [1, 3, 5, 7, 9]),
    ]
)

NUMERIC_DISTINCT_FILTER_CASES = [
    ("int8_max", "i8 == 127", [11]),
    ("int8_min", "i8 == -128", [12]),
    ("int16_max", "i16 == 32767", [11]),
    ("int32_min", "i32 == -2147483648", [12]),
    ("int64_max", "i64 == 9223372036854775807", [11]),
    ("float_fraction", "f > 127.4 and f < 127.6", [11]),
    ("double_fraction", "d > 1000000000000.2 and d < 1000000000000.3", [11]),
]

UNARY_NOT_FILTER_CASES = [
    ("not_bool_true", "not (active == true)", [1, 3, 5, 7, 9]),
    ("not_json_rank_ge_3", "not (meta['rank'] >= 3)", [1, 2, 11, 12]),
    ("not_nullable_bool_true", "not (nullable_bool == true)", [1, 3, 7, 9]),
]

NULL_FILTER_CASES = [
    ("nullable_i64_is_null", "nullable_i64 is null", [3, 4, 7]),
    ("nullable_i64_is_not_null", "nullable_i64 is not null", [1, 2, 5, 6, 8, 9, 10, 11, 12]),
    ("nullable_varchar_is_null", "nullable_varchar is null", [4, 8]),
    ("nullable_varchar_is_not_null", "nullable_varchar is not null", [1, 2, 3, 5, 6, 7, 9, 10, 11, 12]),
    ("nullable_bool_is_null", "nullable_bool is null", [5]),
    ("nullable_bool_is_not_null", "nullable_bool is not null", [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12]),
    ("nullable_arr_i64_is_null", "nullable_arr_i64 is null", [6]),
    ("nullable_arr_i64_is_not_null", "nullable_arr_i64 is not null", [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12]),
    ("meta_nullable_is_null", "meta_nullable is null", [9]),
    ("meta_nullable_is_not_null", "meta_nullable is not null", [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]),
]

JSON_KEY_NULL_FILTER_CASES = [
    ("json_present_null_is_null", 'meta["maybe_null"] is null', [2]),
    (
        "json_present_null_is_not_null",
        'meta["maybe_null"] is not null',
        [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    ),
    ("json_missing_key_is_null", 'meta["missing_key"] is null', list(range(1, 13))),
    ("json_missing_key_is_not_null", 'meta["missing_key"] is not null', []),
    ("json_missing_key_outer_not_eq", 'not (meta["missing_key"] == 1)', []),
]

UNKNOWN_BOOLEAN_COMPOSITION_CASES = [
    ("unknown_or_true", 'meta["missing_key"] == 1 or id in [1, 2]', [1, 2]),
    ("unknown_and_true", 'meta["missing_key"] == 1 and id in [1, 2]', []),
    ("not_unknown_or_true", 'not ((meta["missing_key"] == 1) or id == 1)', []),
    ("is_null_then_not_eq_unknown", 'meta["missing_key"] is null and not (meta["missing_key"] == 1)', []),
    (
        "not_json_key_is_null",
        'not (meta["maybe_null"] is null)',
        [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    ),
]

ARITHMETIC_EXTENDED_FILTER_CASES = [
    (
        "mod_div_pow_precedence_mix",
        "(i64 % 2 == 0 and i64 / 2 >= 2) or (2 ** 3 == 8 and i64 == 3)",
        [3, 4, 6, 8, 10],
    ),
    (
        "constant_power_false_control",
        "(2 ** 3 == 7 and i64 == 3) or i64 == 4",
        [4],
    ),
]

BOOLEAN_COMBINATORIAL_STRESS_CASES = [
    (
        "deep_nested_mixed_scalar_array_json_tree",
        "(((i64 in [1, 2, 3, 4, 5] and active == true) "
        'or (array_contains(arr_i64, 3) and meta["group"] == "qa")) '
        "and not (nullable_i64 is null)) "
        'or (meta["rank"] >= 9 and name like "user_%")',
        [2, 5, 9],
    ),
]

ARRAY_FILTER_CASES = [
    ("arr_i64_contains_3", "array_contains(arr_i64, 3)", [3, 4, 5, 11]),
    ("arr_i64_contains_any", "array_contains_any(arr_i64, [3, 7])", [3, 4, 5, 7, 8, 9, 11]),
    ("arr_i64_contains_all", "array_contains_all(arr_i64, [3, 4])", [4, 5]),
    ("arr_varchar_contains_red", 'array_contains(arr_varchar, "red")', [1, 3]),
    (
        "arr_varchar_contains_any",
        'array_contains_any(arr_varchar, ["red", "blue"])',
        [1, 2, 3, 4],
    ),
]

ARRAY_LENGTH_FILTER_CASES = [
    ("arr_i64_length_3", "array_length(arr_i64) == 3", list(range(1, 10))),
    ("nullable_arr_i64_length_0", "array_length(nullable_arr_i64) == 0", [10]),
    (
        "nullable_arr_i64_length_1",
        "array_length(nullable_arr_i64) == 1",
        [1, 2, 3, 4, 5, 7, 8, 9, 11, 12],
    ),
]

ARRAY_OTHER_TYPE_FILTER_CASES = [
    ("arr_float_contains_3_5", "array_contains(arr_float, 3.5)", [3]),
    ("arr_double_contains_any", "array_contains_any(arr_double, [2.25, 9.25])", [2, 9]),
    ("arr_bool_contains_true", "array_contains(arr_bool, true)", [1, 4, 6, 8, 9, 10, 11]),
    ("arr_bool_contains_false", "array_contains(arr_bool, false)", [2, 3, 5, 7, 8, 9, 12]),
]

ARRAY_NULL_EMPTY_FILTER_CASES = [
    ("nullable_arr_i64_is_null", "nullable_arr_i64 is null", [6]),
    ("nullable_arr_i64_is_not_null", "nullable_arr_i64 is not null", [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12]),
    ("nullable_arr_i64_positive_control", "array_contains(nullable_arr_i64, 5)", [5]),
    (
        "nullable_arr_i64_empty_not_contains",
        "not array_contains(nullable_arr_i64, 10)",
        [1, 2, 3, 4, 5, 7, 8, 9, 10, 12],
    ),
]

ORDER_ARRAY_FUNCTION_EXPRESSIONS = [
    (
        "array_contains(arr_i64, 3) and active == true",
        "active == true and array_contains(arr_i64, 3)",
        [4, 11],
    ),
]

JSON_ARRAY_FILTER_CASES = [
    ("json_rank_ge_3", 'meta["rank"] >= 3', [3, 4, 5, 6, 7, 8, 9, 10]),
    ("json_group_in", 'meta["group"] in ["qa", "dev"]', [1, 2, 4, 5, 7, 8, 10]),
    ("json_contains_hot", 'json_contains(meta["labels"], "hot")', [1, 3, 5]),
]

JSON_ARRAY_COMPOSITION_CASES = [
    (
        "array_len_and_unknown_or_true",
        'array_length(nullable_arr_i64) == 1 and (meta["missing_key"] == 1 or id in [1, 10])',
        [1],
    ),
    ("json_array_contains_string", 'array_contains(meta["labels"], "hot")', [1, 3, 5]),
    (
        "json_array_length_and_contains",
        'array_length(meta["labels"]) == 1 and array_contains(meta["labels"], "hot")',
        [1, 5],
    ),
]

NON_EMPTY_TEMPLATE_CONTROL_CASES = [
    ("scalar_in", "id <= 2 and id in {values}", {"values": [1]}, [1]),
    ("scalar_not_in", "id <= 2 and id not in {values}", {"values": [1]}, [2]),
    (
        "array_contains_any",
        "id <= 2 and array_contains_any(tags, {values})",
        {"values": ["blue"]},
        [2],
    ),
    (
        "json_contains_any",
        'id <= 2 and json_contains_any(meta["tags"], {values})',
        {"values": ["blue"]},
        [2],
    ),
]

NEGATIVE_FILTER_ERROR_CASES = [
    ("unknown_field_in", "invalid_field in [1, 2]", "field invalid_field not exist"),
    ("array_contains_wrong_literal_type", 'array_contains(arr_i64, "not_an_int")', "cannot cast value to Int64"),
    ("invalid_json_path_syntax", "meta['rank' >= 3", "invalid expression"),
    (
        "geometry_func_on_non_geometry_field",
        "ST_WITHIN(name, 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')",
        "spatial operation are only supported on geometry fields",
    ),
    (
        "cross_field_arithmetic_rejected",
        "i64 + i32 == 6",
        "not supported to do arithmetic operations between multiple fields",
    ),
    (
        "array_length_arithmetic_rejected",
        "i64 + array_length(arr_i64) == 6",
        "complicated arithmetic operations are not supported",
    ),
    (
        "field_power_arithmetic_rejected",
        "i64 ** 2 == 9",
        "power can only apply on constants",
    ),
]

NEGATIVE_FILTER_ERROR_PENDING_CASES = []

INDEX_NEGATIVE_ERROR_CASES = [
    (
        "ngram_missing_gram_params",
        {"field_name": "name_indexed", "index_type": "NGRAM", "params": {}},
        "Ngram index must specify both min_gram and max_gram",
    ),
    (
        "json_path_index_missing_cast_type",
        {
            "field_name": "meta_indexed",
            "index_type": "INVERTED",
            "params": {"json_path": "meta_indexed['rank']"},
        },
        "json index must specify cast type",
    ),
    (
        "json_path_index_unsupported_array_int64_cast_type",
        {
            "field_name": "meta_indexed",
            "index_type": "INVERTED",
            "params": {"json_path": "meta_indexed['scores']", "json_cast_type": "ARRAY_INT64"},
        },
        "json_cast_type ARRAY_INT64 is not supported",
    ),
    (
        "ngram_json_path_missing_cast_type",
        {
            "field_name": "meta_indexed",
            "index_type": "NGRAM",
            "params": {"json_path": "meta_indexed['name']", "min_gram": 2, "max_gram": 3},
        },
        "JSON field with ngram index must specify json_cast_type",
    ),
    (
        "ngram_min_greater_than_max",
        {"field_name": "name_indexed", "index_type": "NGRAM", "params": {"min_gram": 4, "max_gram": 2}},
        "invalid min_gram or max_gram value",
    ),
]

INDEX_NEGATIVE_ERROR_PENDING_CASES = []


INDEX_CONSISTENCY_CASES = [
    {
        "case_name": "int64_inverted_arithmetic",
        "field_type": "INT64",
        "index_type": "INVERTED",
        "plain_expr": "i64_plain + 5 >= 36 and i64_plain + 5 <= 76",
        "indexed_expr": "i64_indexed + 5 >= 36 and i64_indexed + 5 <= 76",
        "expected_ids": [3, 4, 5, 6, 7],
    },
    {
        "case_name": "int64_inverted_range",
        "field_type": "INT64",
        "index_type": "INVERTED",
        "plain_expr": "i64_plain >= 31 and i64_plain <= 71",
        "indexed_expr": "i64_indexed >= 31 and i64_indexed <= 71",
        "expected_ids": [3, 4, 5, 6, 7],
    },
    {
        "case_name": "int64_bitmap_in",
        "field_type": "INT64",
        "index_type": "BITMAP",
        "plain_expr": "i64_bitmap_plain in [10002, 10004, 10006, 10008]",
        "indexed_expr": "i64_bitmap_indexed in [10002, 10004, 10006, 10008]",
        "expected_ids": [2, 4, 6, 8],
    },
    {
        "case_name": "varchar_ngram_like",
        "field_type": "VARCHAR",
        "index_type": "NGRAM",
        "plain_expr": 'name_plain like "%svc%"',
        "indexed_expr": 'name_indexed like "%svc%"',
        "expected_ids": [2, 4, 6, 8, 10],
    },
    {
        "case_name": "varchar_trie_prefix_like",
        "field_type": "VARCHAR",
        "index_type": "TRIE",
        "plain_expr": 'name_trie_plain like r"svc\\_%"',
        "indexed_expr": 'name_trie_indexed like r"svc\\_%"',
        "expected_ids": [2, 4, 6, 8, 10, 12],
    },
    {
        "case_name": "json_rank_double_inverted",
        "field_type": "JSON",
        "index_type": "INVERTED",
        "plain_expr": "meta_plain['rank'] >= 3",
        "indexed_expr": "meta_rank_indexed['rank'] >= 3",
        "expected_ids": [3, 4, 5, 6, 7, 8, 9, 10],
    },
    {
        "case_name": "json_group_varchar_inverted",
        "field_type": "JSON",
        "index_type": "INVERTED",
        "plain_expr": 'meta_plain["group"] == "qa"',
        "indexed_expr": 'meta_group_indexed["group"] == "qa"',
        "expected_ids": [1, 4, 7, 10],
    },
    {
        "case_name": "json_active_bool_inverted",
        "field_type": "JSON",
        "index_type": "INVERTED",
        "plain_expr": "meta_plain['active'] == true",
        "indexed_expr": "meta_active_indexed['active'] == true",
        "expected_ids": [1, 2, 4],
    },
    {
        "case_name": "bitset_boundary_63_65",
        "field_type": "INT64",
        "index_type": "INVERTED",
        "plain_expr": "i64_plain in [631, 641, 651]",
        "indexed_expr": "i64_indexed in [631, 641, 651]",
        "expected_ids": [63, 64, 65],
    },
    {
        "case_name": "row_id_boundary_1023_1025",
        "field_type": "INT64",
        "index_type": "INVERTED",
        "plain_expr": "i64_plain in [10231, 10241, 10251]",
        "indexed_expr": "i64_indexed in [10231, 10241, 10251]",
        "expected_ids": [1023, 1024, 1025],
    },
    {
        "case_name": "json_array_double_contains",
        "field_type": "JSON_ARRAY",
        "index_type": "INVERTED",
        "plain_expr": "array_contains(meta_arr_plain['scores'], 3.25)",
        "indexed_expr": "array_contains(meta_arr_indexed['scores'], 3.25)",
        "expected_ids": [3],
    },
    {
        "case_name": "json_array_double_contains_any",
        "field_type": "JSON_ARRAY",
        "index_type": "INVERTED",
        "plain_expr": "array_contains_any(meta_arr_plain['scores'], [2.25, 9.75])",
        "indexed_expr": "array_contains_any(meta_arr_indexed['scores'], [2.25, 9.75])",
        "expected_ids": [2, 9],
    },
    {
        "case_name": "json_array_double_exact_array",
        "field_type": "JSON_ARRAY",
        "index_type": "INVERTED",
        "plain_expr": "meta_arr_plain['scores'] == [6.25, 16.5]",
        "indexed_expr": "meta_arr_indexed['scores'] == [6.25, 16.5]",
        "expected_ids": [6],
    },
]


def build_filter_matrix_rows(pk_field="id"):
    group_by_id = {
        1: "qa",
        2: "dev",
        3: "ops",
        4: "qa",
        5: "dev",
        6: "ops",
        7: "qa",
        8: "dev",
        9: "ops",
        10: "qa",
    }
    arr_varchar_by_id = {
        1: ["green", "red", "amber"],
        2: ["green", "blue", "amber"],
        3: ["red"],
        4: ["blue"],
        5: ["green"],
        6: ["green"],
        7: ["yellow"],
        8: ["yellow"],
        9: ["black"],
        10: ["white"],
    }
    arr_float_controls = {
        3: [30.0, 3.5, 31.0],
        4: [3.0, 40.5],
    }
    arr_double_controls = {
        2: [20.0, 2.25, 21.0],
        3: [2.0, 30.25],
        9: [90.0, 9.25, 91.0],
        10: [9.0, 100.25],
    }
    arr_bool_controls = {
        1: [True, True],
        2: [False, False],
        8: [False, True, False],
        9: [True, False, True],
    }
    rows = []
    for i in range(1, 11):
        rows.append(
            {
                pk_field: i,
                **{field: numeric_scalar_value(field, i) for field in NUMERIC_SCALAR_FIELDS},
                "active": i % 2 == 0,
                "nullable_i64": None if i in {3, 4, 7} else i,
                "nullable_varchar": None if i in {4, 8} else f"nullable_{i}",
                "nullable_bool": None if i == 5 else i % 2 == 0,
                "name": "system_10" if i == 10 else f"user_{i}",
                "arr_i64": [10] if i == 10 else [i - 2, i - 1, i],
                "arr_float": arr_float_controls.get(i, [float(i), float(i) + 0.5]),
                "arr_double": arr_double_controls.get(i, [float(i), float(i) + 0.25]),
                "arr_bool": arr_bool_controls.get(i, [i % 2 == 0]),
                "arr_varchar": arr_varchar_by_id[i],
                "nullable_arr_i64": None if i == 6 else ([] if i == 10 else [i]),
                "meta": {
                    "rank": i,
                    "group": "qa" if i == 5 else group_by_id[i],
                    "labels": ["warm", "hot", "cold"] if i == 3 else (["hot"] if i in {1, 5} else ["cold"]),
                    "maybe_null": None if i == 2 else i,
                },
                "meta_nullable": None if i == 9 else {"rank": i},
            }
        )
    rows.extend(
        [
            {
                pk_field: 11,
                **NUMERIC_SCALAR_SENTINEL_VALUES[11],
                "active": True,
                "nullable_i64": 11,
                "nullable_varchar": "nullable_11",
                "nullable_bool": True,
                "name": "control_11",
                "arr_i64": [3],
                "arr_float": [100.5],
                "arr_double": [100.25],
                "arr_bool": [True, True],
                "arr_varchar": ["control"],
                "nullable_arr_i64": [10],
                "meta": {"rank": 0, "group": "control", "labels": ["cold"], "maybe_null": 11},
                "meta_nullable": {"rank": 0},
            },
            {
                pk_field: 12,
                **NUMERIC_SCALAR_SENTINEL_VALUES[12],
                "active": True,
                "nullable_i64": 12,
                "nullable_varchar": "nullable_12",
                "nullable_bool": True,
                "name": "control_12",
                "arr_i64": [-100],
                "arr_float": [-100.5],
                "arr_double": [-100.25],
                "arr_bool": [False, False],
                "arr_varchar": ["control"],
                "nullable_arr_i64": [-100],
                "meta": {"rank": 0, "group": "control", "labels": ["cold"], "maybe_null": 12},
                "meta_nullable": {"rank": 0},
            },
        ]
    )
    return rows


def build_timestamptz_rows(pk_field="id", use_dual_fields=False, row_count=5):
    values = {
        1: "2025-01-01T00:00:00Z",
        2: "2025-01-01T08:00:00+08:00",
        3: "2025-01-01T12:30:00Z",
        4: "2025-01-02T00:00:00Z",
        5: None,
        1023: "2025-01-01T00:00:00Z",
        1024: "2025-01-01T12:30:00Z",
        2047: None,
        2999: "2025-01-02T00:00:00Z",
        3000: "2030-01-01T00:00:00Z",
    }
    rows = []
    for pk in range(1, row_count + 1):
        value = values.get(pk)
        row = {pk_field: pk}
        if use_dual_fields:
            row["event_time_plain"] = value
            row["event_time_indexed"] = value
        else:
            row["event_time"] = value
        rows.append(row)
    return rows


TIMESTAMPTZ_FILTER_CASES = [
    ("event_time_plain == ISO '2025-01-01T00:00:00Z'", [1, 2, 1023]),
    (
        "event_time_plain >= ISO '2025-01-01T00:00:00Z' and event_time_plain < ISO '2025-01-02T00:00:00Z'",
        [1, 2, 3, 1023, 1024],
    ),
    (
        "(event_time_plain == ISO '2025-01-01T00:00:00Z' or event_time_plain == ISO '2025-01-02T00:00:00Z')",
        [1, 2, 4, 1023, 2999],
    ),
    ("id in [1, 5, 2047, 3000] and event_time_plain is null", [5, 2047]),
    (
        "id in [1, 5, 2047, 3000] and event_time_plain is not null",
        [1, 3000],
    ),
]


TIMESTAMPTZ_INTERVAL_51538_CASES = [
    (
        "day_non_null_control",
        "id <= 4 and event_time_plain + INTERVAL 'P1D' < ISO '2025-01-03T00:00:00Z'",
        [1, 2, 3],
    ),
    (
        "month_non_null_control",
        "id <= 4 and event_time_plain + INTERVAL 'P1M' < ISO '2025-02-02T00:00:00Z'",
        [1, 2, 3],
    ),
    (
        "year_non_null_control",
        "id <= 4 and event_time_plain + INTERVAL 'P1Y' < ISO '2026-01-02T00:00:00Z'",
        [1, 2, 3],
    ),
    (
        "day_null_positive",
        "id <= 5 and event_time_plain + INTERVAL 'P1D' < ISO '2000-01-01T00:00:00Z'",
        [],
    ),
    (
        "day_null_outer_not",
        "id <= 5 and not (event_time_plain + INTERVAL 'P1D' < ISO '1900-01-01T00:00:00Z')",
        [1, 2, 3, 4],
    ),
    (
        "month_null_positive",
        "id <= 5 and event_time_plain + INTERVAL 'P1M' < ISO '2000-01-01T00:00:00Z'",
        [],
    ),
    (
        "month_null_outer_not",
        "id <= 5 and not (event_time_plain + INTERVAL 'P1M' < ISO '1900-01-01T00:00:00Z')",
        [1, 2, 3, 4],
    ),
    (
        "year_null_positive",
        "id <= 5 and event_time_plain + INTERVAL 'P1Y' < ISO '2000-01-01T00:00:00Z'",
        [],
    ),
    (
        "year_null_outer_not",
        "id <= 5 and not (event_time_plain + INTERVAL 'P1Y' < ISO '1900-01-01T00:00:00Z')",
        [1, 2, 3, 4],
    ),
]

TIMESTAMPTZ_INDEX_CONSISTENCY_CASES = [
    (
        "eq_utc",
        "event_time_plain == ISO '2025-01-01T00:00:00Z'",
        "event_time_indexed == ISO '2025-01-01T00:00:00Z'",
        [1, 2, 1023],
    ),
    (
        "range_same_day",
        "event_time_plain >= ISO '2025-01-01T00:00:00Z' and event_time_plain < ISO '2025-01-02T00:00:00Z'",
        "event_time_indexed >= ISO '2025-01-01T00:00:00Z' and event_time_indexed < ISO '2025-01-02T00:00:00Z'",
        [1, 2, 3, 1023, 1024],
    ),
    (
        "or_two_instants",
        "(event_time_plain == ISO '2025-01-01T00:00:00Z' or event_time_plain == ISO '2025-01-02T00:00:00Z')",
        "(event_time_indexed == ISO '2025-01-01T00:00:00Z' or event_time_indexed == ISO '2025-01-02T00:00:00Z')",
        [1, 2, 4, 1023, 2999],
    ),
    (
        "is_null",
        "id in [1, 5, 2047, 3000] and event_time_plain is null",
        "id in [1, 5, 2047, 3000] and event_time_indexed is null",
        [5, 2047],
    ),
    (
        "is_not_null",
        "id in [1, 5, 2047, 3000] and event_time_plain is not null",
        "id in [1, 5, 2047, 3000] and event_time_indexed is not null",
        [1, 3000],
    ),
]

TEXT_ROWS = [
    {"id": 1, "content": "vector database filtering", "topic": "db"},
    {"id": 2, "content": "distributed database storage", "topic": "db"},
    {"id": 3, "content": "cloud infra observability", "topic": "infra"},
    {"id": 4, "content": "vector search ranking", "topic": "search"},
    {"id": 5, "content": "database backup infra", "topic": "infra"},
    {"id": 6, "content": "database vector migration", "topic": "other"},
    {"id": 7, "content": "database operations handbook", "topic": "other"},
    {"id": 8, "content": "vector storage handbook", "topic": "db"},
]

TEXT_FILTER_CASES = [
    ("text_match(content, 'database')", [1, 2, 5, 6, 7]),
    ("phrase_match(content, 'vector database', 0)", [1]),
    ("text_match(content, 'database') and topic == 'db'", [1, 2]),
    ("text_match(content, 'database') or topic == 'infra'", [1, 2, 3, 5, 6, 7]),
]

QUERY_POLYGON_ALL = "POLYGON ((-10 -10, 110 -10, 110 110, -10 110, -10 -10))"
QUERY_POINT_INSIDE_FIRST_POLYGON = "POINT (15 15)"
QUERY_POINT_ON_FIRST_POLYGON_BOUNDARY = "POINT (0 10)"
REFINEMENT_HOLE_POLYGON = (
    "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0), (8 8, 8 12, 12 12, 12 8, 8 8), (14 14, 14 16, 16 16, 16 14, 14 14))"
)

GEOMETRY_ROWS = [
    {"id": 1, "geo_plain": "POINT (10 10)", "geo_indexed": "POINT (10 10)"},
    {"id": 2, "geo_plain": "POINT (40 40)", "geo_indexed": "POINT (40 40)"},
    {"id": 3, "geo_plain": "POINT (70 70)", "geo_indexed": "POINT (70 70)"},
    {
        "id": 4,
        "geo_plain": "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))",
        "geo_indexed": "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))",
    },
    {
        "id": 5,
        "geo_plain": "POLYGON ((60 60, 90 60, 90 90, 60 90, 60 60))",
        "geo_indexed": "POLYGON ((60 60, 90 60, 90 90, 60 90, 60 60))",
    },
    {
        "id": 6,
        "geo_plain": "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))",
        "geo_indexed": "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))",
    },
    {"id": 7, "geo_plain": "POINT (10.000001 10)", "geo_indexed": "POINT (10.000001 10)"},
    {
        "id": 8,
        "geo_plain": REFINEMENT_HOLE_POLYGON,
        "geo_indexed": REFINEMENT_HOLE_POLYGON,
    },
]


def build_geometry_rows(row_count=5):
    special_rows = {row["id"]: row for row in GEOMETRY_ROWS}
    special_rows.update(
        {
            1023: {"id": 1023, "geo_plain": "POINT (10 10)", "geo_indexed": "POINT (10 10)"},
            1024: {
                "id": 1024,
                "geo_plain": "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))",
                "geo_indexed": "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))",
            },
            2047: {
                "id": 2047,
                "geo_plain": "POINT (10.000001 10)",
                "geo_indexed": "POINT (10.000001 10)",
            },
            2999: {
                "id": 2999,
                "geo_plain": "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))",
                "geo_indexed": "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))",
            },
        }
    )
    rows = []
    for i in range(1, row_count + 1):
        if i in special_rows:
            rows.append(special_rows[i])
            continue
        offset = (i % 10) * 0.001
        point = f"POINT ({-80.0 + offset} {-80.0 + offset})"
        rows.append({"id": i, "geo_plain": point, "geo_indexed": point})
    return rows


GEOMETRY_INDEX_CONSISTENCY_CASES = [
    (
        "within_all",
        f"ST_WITHIN(geo_plain, '{QUERY_POLYGON_ALL}')",
        f"ST_WITHIN(geo_indexed, '{QUERY_POLYGON_ALL}')",
        [1, 2, 3, 4, 5, 6, 7, 8, 1023, 1024, 2047, 2999],
    ),
    (
        "within_hole_refinement",
        f"ST_WITHIN(geo_plain, '{REFINEMENT_HOLE_POLYGON}')",
        f"ST_WITHIN(geo_indexed, '{REFINEMENT_HOLE_POLYGON}')",
        [8],
    ),
    (
        "intersects_hole_refinement",
        f"ST_INTERSECTS(geo_plain, '{QUERY_POINT_INSIDE_FIRST_POLYGON}')",
        f"ST_INTERSECTS(geo_indexed, '{QUERY_POINT_INSIDE_FIRST_POLYGON}')",
        [4, 2999],
    ),
    (
        "contains_point",
        f"ST_CONTAINS(geo_plain, '{QUERY_POINT_INSIDE_FIRST_POLYGON}')",
        f"ST_CONTAINS(geo_indexed, '{QUERY_POINT_INSIDE_FIRST_POLYGON}')",
        [4, 2999],
    ),
    (
        "intersects_boundary_point",
        f"ST_INTERSECTS(geo_plain, '{QUERY_POINT_ON_FIRST_POLYGON_BOUNDARY}')",
        f"ST_INTERSECTS(geo_indexed, '{QUERY_POINT_ON_FIRST_POLYGON_BOUNDARY}')",
        [4, 8, 2999],
    ),
    (
        "contains_boundary_point",
        f"ST_CONTAINS(geo_plain, '{QUERY_POINT_ON_FIRST_POLYGON_BOUNDARY}')",
        f"ST_CONTAINS(geo_indexed, '{QUERY_POINT_ON_FIRST_POLYGON_BOUNDARY}')",
        [],
    ),
    (
        "equals_point",
        "ST_EQUALS(geo_plain, 'POINT (10 10)')",
        "ST_EQUALS(geo_indexed, 'POINT (10 10)')",
        [1, 1023],
    ),
    (
        "dwithin_point_one_meter",
        "ST_DWITHIN(geo_plain, 'POINT (10 10)', 1)",
        "ST_DWITHIN(geo_indexed, 'POINT (10 10)', 1)",
        [1, 4, 7, 1023, 2047, 2999],
    ),
]
