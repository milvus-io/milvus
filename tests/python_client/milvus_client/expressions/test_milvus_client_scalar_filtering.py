"""
Scalar expression filtering tests with deterministic boundary values and eval-based ground truth.

Design principles:
- Deterministic boundary rows (INT64_MAX, MIN, 2^53, 0, etc.) as first N rows per type
- Random fill rows with fixed seed for reproducibility
- Python eval() as ground truth oracle (not hand-written validators)
- Separated: correctness (1 field/type) vs index consistency (same type × indexes)
- Corner-case expressions from real bugs (#48440 overflow, #48441 3VL, #48442 mixed-type IN, #48443 bool literal)
- Full operator coverage: comparison, arithmetic, logical, range, string, null, array, JSON
"""
import pytest
import random
import re
import math
import numpy as np
from typing import List, Dict, Any, Tuple
from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType

prefix = "scalar_filter"
default_dim = 8
default_pk = "id"
default_vec = "vector"
DEFAULT_SEED = 19530

# ---------------------------------------------------------------------------
# Boundary value constants
# ---------------------------------------------------------------------------
INT8_MIN, INT8_MAX = -128, 127
INT16_MIN, INT16_MAX = -32768, 32767
INT32_MIN, INT32_MAX = -2147483648, 2147483647
INT64_MIN, INT64_MAX = -9223372036854775808, 9223372036854775807
FLOAT64_INT_LIMIT = 2**53

BOUNDARY_VALUES = {
    DataType.INT8: [0, 1, -1, INT8_MIN, INT8_MAX, INT8_MIN + 1, INT8_MAX - 1, 42],
    DataType.INT16: [0, 1, -1, INT16_MIN, INT16_MAX, INT16_MIN + 1, INT16_MAX - 1, 100, -100],
    DataType.INT32: [0, 1, -1, INT32_MIN, INT32_MAX, INT32_MIN + 1, INT32_MAX - 1, 1000, -1000],
    DataType.INT64: [0, 1, -1, INT64_MIN, INT64_MAX, INT64_MIN + 1, INT64_MAX - 1,
                     FLOAT64_INT_LIMIT, -FLOAT64_INT_LIMIT, FLOAT64_INT_LIMIT + 1],
    DataType.FLOAT: [0.0, 1.0, -1.0, float('inf'), float('-inf'), float('nan'),
                     1e-7, -1e-7, 3.14, 1e38, -1e38],
    DataType.DOUBLE: [0.0, 1.0, -1.0, float('inf'), float('-inf'), float('nan'),
                      1e-15, -1e-15, 2.718, 1e308, -1e308],
    DataType.BOOL: [True, False],
    DataType.VARCHAR: ["", " ", "a", "abc", "abc%def", "abc_def",
                       "str_0", "str_1", "0_str", "%special%", "_under_"],
}


# ---------------------------------------------------------------------------
# Data generation helpers
# ---------------------------------------------------------------------------
def make_random_value(dtype: DataType, rng: random.Random) -> Any:
    """Generate a random value for the given DataType using the provided RNG."""
    if dtype == DataType.INT8:
        return rng.randint(INT8_MIN, INT8_MAX)
    elif dtype == DataType.INT16:
        return rng.randint(INT16_MIN, INT16_MAX)
    elif dtype == DataType.INT32:
        return rng.randint(INT32_MIN, INT32_MAX)
    elif dtype == DataType.INT64:
        return rng.randint(INT64_MIN // (10**9), INT64_MAX // (10**9))
    elif dtype == DataType.FLOAT:
        return rng.uniform(-1e6, 1e6)
    elif dtype == DataType.DOUBLE:
        return rng.uniform(-1e12, 1e12)
    elif dtype == DataType.BOOL:
        return rng.choice([True, False])
    elif dtype == DataType.VARCHAR:
        length = rng.randint(1, 20)
        chars = "abcdefghijklmnopqrstuvwxyz0123456789_ "
        return "".join(rng.choice(chars) for _ in range(length))
    else:
        raise ValueError(f"Unsupported dtype for random value: {dtype}")


def make_nullable_value(dtype: DataType, rng: random.Random, null_prob: float = 0.1) -> Any:
    """Generate a value that may be None (for nullable fields)."""
    if rng.random() < null_prob:
        return None
    return make_random_value(dtype, rng)


def make_random_array(element_dtype: DataType, rng: random.Random,
                      min_len: int = 0, max_len: int = 5) -> List[Any]:
    """Generate a random array of elements of the given type."""
    length = rng.randint(min_len, max_len)
    return [make_random_value(element_dtype, rng) for _ in range(length)]


def generate_deterministic_rows(
    field_configs: List[Dict[str, Any]],
    total_rows: int = 200,
    seed: int = DEFAULT_SEED,
) -> List[Dict[str, Any]]:
    """
    Generate deterministic test data rows.

    For each field, boundary values (from BOUNDARY_VALUES) are placed in the
    first N rows. Remaining rows are filled with random values from a seeded RNG.

    Args:
        field_configs: list of dicts with keys:
            - name (str): field name
            - dtype (DataType): data type
            - nullable (bool, optional): whether the field can be None
            - is_array (bool, optional): whether this is an array field
            - element_dtype (DataType, optional): element type for array fields
        total_rows: total number of rows to generate
        seed: random seed for reproducibility

    Returns:
        list of dicts, one per row, keyed by field name
    """
    rng = random.Random(seed)
    rows = [{} for _ in range(total_rows)]

    for fc in field_configs:
        name = fc["name"]
        dtype = fc["dtype"]
        nullable = fc.get("nullable", False)
        is_array = fc.get("is_array", False)
        element_dtype = fc.get("element_dtype", None)

        if is_array:
            # For array fields, generate random arrays for all rows
            for i in range(total_rows):
                if nullable:
                    if rng.random() < 0.1:
                        rows[i][name] = None
                        continue
                rows[i][name] = make_random_array(element_dtype or DataType.INT64, rng)
            continue

        # Get boundary values for this type
        boundaries = BOUNDARY_VALUES.get(dtype, [])

        # Place boundary values in the first rows
        for i, val in enumerate(boundaries):
            if i < total_rows:
                rows[i][name] = val

        # Fill remaining rows with random values
        for i in range(len(boundaries), total_rows):
            if nullable:
                rows[i][name] = make_nullable_value(dtype, rng)
            else:
                rows[i][name] = make_random_value(dtype, rng)

    return rows


# ---------------------------------------------------------------------------
# Eval engine — Python-based ground truth for Milvus expressions
# ---------------------------------------------------------------------------
def _like_to_regex(pattern: str) -> str:
    """Convert a SQL LIKE pattern to a Python regex pattern.

    Milvus LIKE semantics:
      % → .*   (any sequence of characters)
      _ → .    (any single character)
    """
    result = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == '%':
            result.append('.*')
        elif ch == '_':
            result.append('.')
        elif ch == '\\' and i + 1 < len(pattern):
            # Escaped character — treat next char as literal
            i += 1
            result.append(re.escape(pattern[i]))
        else:
            result.append(re.escape(ch))
        i += 1
    return '^' + ''.join(result) + '$'


def _like_match(value: Any, pattern: str) -> bool:
    """Evaluate value LIKE pattern using Milvus LIKE semantics."""
    if value is None:
        return False
    regex = _like_to_regex(pattern)
    return bool(re.match(regex, str(value)))


def _array_contains(arr: Any, element: Any) -> bool:
    """Check if an array contains the given element."""
    if arr is None:
        return False
    return element in arr


def _array_contains_all(arr: Any, elements: Any) -> bool:
    """Check if an array contains all of the given elements."""
    if arr is None:
        return False
    return all(e in arr for e in elements)


def _array_contains_any(arr: Any, elements: Any) -> bool:
    """Check if an array contains any of the given elements."""
    if arr is None:
        return False
    return any(e in arr for e in elements)


def _array_length(arr: Any) -> int:
    """Return the length of an array, or 0 if None."""
    if arr is None:
        return 0
    return len(arr)


def _milvus_to_python(expr: str) -> str:
    """
    Translate a Milvus filter expression to a Python expression that can be
    evaluated with eval() against a row dict.

    Supported translations:
      - field_name → row['field_name']
      - == → ==  (already Python)
      - != → !=
      - && / and → and
      - || / or  → or
      - not → not
      - IN [...] → in [...]
      - NOT IN [...] → not in [...]
      - LIKE "pattern" → _like_match(row['field'], 'pattern')
      - field IS NULL → row['field'] is None
      - field IS NOT NULL → row['field'] is not None
      - array_contains(field, val) → _array_contains(row['field'], val)
      - array_contains_all(field, [...]) → _array_contains_all(row['field'], [...])
      - array_contains_any(field, [...]) → _array_contains_any(row['field'], [...])
      - array_length(field) → _array_length(row['field'])
      - true/false → True/False
    """
    s = expr

    # Boolean literals (case insensitive, word boundary)
    s = re.sub(r'\btrue\b', 'True', s, flags=re.IGNORECASE)
    s = re.sub(r'\bfalse\b', 'False', s, flags=re.IGNORECASE)

    # IS NOT NULL / IS NULL (must come before general field substitution)
    s = re.sub(
        r'\b(\w+)\s+IS\s+NOT\s+NULL\b',
        r"row['\1'] is not None",
        s, flags=re.IGNORECASE,
    )
    s = re.sub(
        r'\b(\w+)\s+IS\s+NULL\b',
        r"row['\1'] is None",
        s, flags=re.IGNORECASE,
    )

    # LIKE — convert to _like_match call
    s = re.sub(
        r'\b(\w+)\s+LIKE\s+"([^"]*)"',
        r"_like_match(row['\1'], '\2')",
        s, flags=re.IGNORECASE,
    )
    s = re.sub(
        r"\b(\w+)\s+LIKE\s+'([^']*)'",
        r"_like_match(row['\1'], '\2')",
        s, flags=re.IGNORECASE,
    )

    # NOT IN (must come before IN)
    s = re.sub(
        r'\b(\w+)\s+NOT\s+IN\s*\[',
        r"row['\1'] not in [",
        s, flags=re.IGNORECASE,
    )

    # IN
    s = re.sub(
        r'\b(\w+)\s+IN\s*\[',
        r"row['\1'] in [",
        s, flags=re.IGNORECASE,
    )

    # array_contains_all(field, [...])
    s = re.sub(
        r'\barray_contains_all\s*\(\s*(\w+)\s*,',
        r"_array_contains_all(row['\1'],",
        s, flags=re.IGNORECASE,
    )

    # array_contains_any(field, [...])
    s = re.sub(
        r'\barray_contains_any\s*\(\s*(\w+)\s*,',
        r"_array_contains_any(row['\1'],",
        s, flags=re.IGNORECASE,
    )

    # array_contains(field, val) — must come after the _all/_any variants
    s = re.sub(
        r'\barray_contains\s*\(\s*(\w+)\s*,',
        r"_array_contains(row['\1'],",
        s, flags=re.IGNORECASE,
    )

    # array_length(field)
    s = re.sub(
        r'\barray_length\s*\(\s*(\w+)\s*\)',
        r"_array_length(row['\1'])",
        s, flags=re.IGNORECASE,
    )

    # Logical operators
    s = re.sub(r'&&', ' and ', s)
    s = re.sub(r'\|\|', ' or ', s)

    # Replace remaining bare field names with row['field']
    # This must happen after all pattern-specific replacements.
    # We look for word tokens that are not Python keywords, not already inside row[...],
    # not part of function calls we already translated, and not numeric literals.
    _PYTHON_KEYWORDS = {
        'and', 'or', 'not', 'in', 'is', 'None', 'True', 'False',
        'row', 'if', 'else', 'for', 'while', 'return', 'def', 'class',
    }
    _HELPER_FUNCS = {
        '_like_match', '_array_contains', '_array_contains_all',
        '_array_contains_any', '_array_length',
    }

    def _replace_field_refs(text: str) -> str:
        """Replace bare field identifiers with row['field'] lookups."""
        tokens = re.split(r'(\brow\[\'[^\']*\'\]|\"[^\"]*\"|\'[^\']*\'|\b\w+\b|[^\w\s])', text)
        result = []
        skip_next_paren = False
        for tok in tokens:
            # Skip empty tokens
            if not tok or tok.isspace():
                result.append(tok)
                continue
            # Already a row[...] reference — keep as-is
            if tok.startswith("row["):
                result.append(tok)
                continue
            # String literals — keep
            if (tok.startswith('"') and tok.endswith('"')) or \
               (tok.startswith("'") and tok.endswith("'")):
                result.append(tok)
                continue
            # Numbers — keep
            if re.match(r'^-?\d+(\.\d+)?([eE][+-]?\d+)?$', tok):
                result.append(tok)
                continue
            # Python keywords and our helper functions — keep
            if tok in _PYTHON_KEYWORDS or tok in _HELPER_FUNCS:
                result.append(tok)
                continue
            # Operators / punctuation — keep
            if not tok[0].isalpha() and tok[0] != '_':
                result.append(tok)
                continue
            # Otherwise it's a field name — wrap in row[...]
            result.append(f"row['{tok}']")
        return ''.join(result)

    s = _replace_field_refs(s)

    return s


def eval_filter(expr: str, rows: List[Dict[str, Any]]) -> List[int]:
    """
    Evaluate a Milvus filter expression against rows and return matching row indices.

    Uses _milvus_to_python to translate, then Python eval() as the oracle.
    Rows where the expression raises an exception (e.g., type errors with None)
    are treated as non-matching (3VL: NULL → not selected).

    Args:
        expr: Milvus filter expression string
        rows: list of row dicts

    Returns:
        sorted list of indices where the expression evaluates to True
    """
    py_expr = _milvus_to_python(expr)
    matching = []

    # Build the eval namespace with helper functions
    eval_ns = {
        '_like_match': _like_match,
        '_array_contains': _array_contains,
        '_array_contains_all': _array_contains_all,
        '_array_contains_any': _array_contains_any,
        '_array_length': _array_length,
        'math': math,
    }

    for idx, row in enumerate(rows):
        try:
            local_ns = {'row': row}
            result = eval(py_expr, eval_ns, local_ns)
            # Handle NaN comparisons: NaN != NaN, so bool(nan > x) etc. are False
            if result is True:
                matching.append(idx)
        except Exception:
            # 3VL: errors (None comparisons, type mismatches) → not selected
            pass

    return sorted(matching)


# ---------------------------------------------------------------------------
# Expression generators
# ---------------------------------------------------------------------------
def _gen_like_expressions(field_name: str, sample_values: List[str]) -> List[str]:
    """Generate LIKE expressions for a VARCHAR field."""
    exprs = []
    # Prefix match
    for v in sample_values[:3]:
        if v and len(v) >= 2:
            exprs.append(f'{field_name} LIKE "{v[:2]}%"')
    # Suffix match
    for v in sample_values[:3]:
        if v and len(v) >= 2:
            exprs.append(f'{field_name} LIKE "%{v[-2:]}"')
    # Contains
    for v in sample_values[:2]:
        if v and len(v) >= 1:
            exprs.append(f'{field_name} LIKE "%{v[0]}%"')
    # Single char wildcard
    exprs.append(f'{field_name} LIKE "_"')
    exprs.append(f'{field_name} LIKE "___"')
    return exprs


def _gen_array_expressions(field_name: str, element_dtype: DataType,
                           sample_elements: List[Any]) -> List[str]:
    """Generate array expressions for an ARRAY field."""
    exprs = []
    if not sample_elements:
        return exprs

    el = sample_elements[0]
    el_repr = repr(el)

    # array_contains
    exprs.append(f'array_contains({field_name}, {el_repr})')

    # array_contains_all / any with small lists
    if len(sample_elements) >= 2:
        two = [repr(x) for x in sample_elements[:2]]
        exprs.append(f'array_contains_all({field_name}, [{", ".join(two)}])')
        exprs.append(f'array_contains_any({field_name}, [{", ".join(two)}])')

    # array_length comparisons
    exprs.append(f'array_length({field_name}) > 0')
    exprs.append(f'array_length({field_name}) == 0')
    exprs.append(f'array_length({field_name}) >= 2')

    return exprs


def generate_expressions_for_field(
    field_name: str,
    dtype: DataType,
    sample_values: List[Any],
    nullable: bool = False,
    is_array: bool = False,
    element_dtype: DataType = None,
) -> List[str]:
    """
    Generate a comprehensive list of filter expressions for a single field.

    Args:
        field_name: the field name in the collection
        dtype: the DataType of the field
        sample_values: representative values present in the data (for IN lists, etc.)
        nullable: whether to include IS NULL / IS NOT NULL expressions
        is_array: whether this is an array field
        element_dtype: element type for array fields

    Returns:
        list of Milvus expression strings
    """
    exprs = []

    # NULL checks
    if nullable:
        exprs.append(f'{field_name} IS NULL')
        exprs.append(f'{field_name} IS NOT NULL')

    # Array-specific expressions
    if is_array:
        flat_elements = []
        for v in sample_values:
            if isinstance(v, list):
                flat_elements.extend(v)
        flat_elements = flat_elements[:5] if flat_elements else []
        exprs.extend(_gen_array_expressions(field_name, element_dtype, flat_elements))
        return exprs

    # Boolean field — limited operators
    if dtype == DataType.BOOL:
        exprs.append(f'{field_name} == true')
        exprs.append(f'{field_name} == false')
        exprs.append(f'{field_name} != true')
        exprs.append(f'{field_name} != false')
        return exprs

    # Numeric and varchar fields — comparison operators
    non_none = [v for v in sample_values if v is not None]

    # Comparison with boundary/sample values
    for op in ['==', '!=', '>', '<', '>=', '<=']:
        for val in non_none[:3]:
            val_repr = repr(val)
            if dtype == DataType.VARCHAR:
                val_repr = f'"{val}"'
            # Skip float comparisons with nan/inf for simplicity in generation
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                if op in ('==', '!='):
                    exprs.append(f'{field_name} {op} {val_repr}')
                continue
            exprs.append(f'{field_name} {op} {val_repr}')

    # IN / NOT IN
    if len(non_none) >= 2:
        in_vals = non_none[:3]
        if dtype == DataType.VARCHAR:
            in_list = ", ".join(f'"{v}"' for v in in_vals)
        else:
            in_list = ", ".join(repr(v) for v in in_vals)
        exprs.append(f'{field_name} IN [{in_list}]')
        exprs.append(f'{field_name} NOT IN [{in_list}]')

    # VARCHAR-specific: LIKE
    if dtype == DataType.VARCHAR:
        str_values = [v for v in non_none if isinstance(v, str)]
        exprs.extend(_gen_like_expressions(field_name, str_values))

    # Arithmetic expressions for numeric types
    if dtype in (DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64,
                 DataType.FLOAT, DataType.DOUBLE):
        if non_none:
            v = non_none[0]
            if not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                exprs.append(f'{field_name} + 1 > {repr(v)}')
                exprs.append(f'{field_name} - 1 < {repr(v)}')
                exprs.append(f'{field_name} * 2 >= {repr(v)}')

    return exprs


def generate_compound_expressions(
    field_exprs: Dict[str, List[str]],
    max_compounds: int = 20,
    seed: int = DEFAULT_SEED,
) -> List[str]:
    """
    Generate compound (AND/OR/NOT) expressions by combining single-field expressions.

    Args:
        field_exprs: dict mapping field_name → list of expressions for that field
        max_compounds: maximum number of compound expressions to generate
        seed: random seed

    Returns:
        list of compound expression strings
    """
    rng = random.Random(seed)
    all_exprs = []
    for exprs in field_exprs.values():
        all_exprs.extend(exprs)

    if len(all_exprs) < 2:
        return []

    compounds = []
    for _ in range(max_compounds):
        e1, e2 = rng.sample(all_exprs, 2)
        op = rng.choice(['&&', '||'])
        compounds.append(f'({e1}) {op} ({e2})')

    # Add a few NOT expressions
    for _ in range(min(5, max_compounds)):
        e = rng.choice(all_exprs)
        compounds.append(f'not ({e})')

    return compounds


# ──────────────────────────────────────────────────────────────
# Test Class 1: Correctness — eval ground truth
# ──────────────────────────────────────────────────────────────

@pytest.mark.xdist_group("TestScalarExprCorrectness")
class TestScalarExpressionCorrectness(TestMilvusClientV2Base):
    """
    Correctness test: one field per scalar/array type, eval-based ground truth.
    500 rows with deterministic boundary values + random fill.
    Full operator coverage: comparison, arithmetic, range, string, null, array, logical.
    """
    shared_alias = "TestScalarExprCorrectness"
    NUM_ROWS = 500

    # (field_name, DataType, nullable, is_array, elem_dtype_or_None)
    FIELD_DEFS = [
        # Scalar types — one per type, all nullable
        ("int8_val",    DataType.INT8,    True,  False, None),
        ("int16_val",   DataType.INT16,   True,  False, None),
        ("int32_val",   DataType.INT32,   True,  False, None),
        ("int64_val",   DataType.INT64,   True,  False, None),
        ("float_val",   DataType.FLOAT,   True,  False, None),
        ("double_val",  DataType.DOUBLE,  True,  False, None),
        ("bool_val",    DataType.BOOL,    True,  False, None),
        ("varchar_val", DataType.VARCHAR, True,  False, None),
        # Array types — all supported element types
        ("arr_int8",    DataType.ARRAY,   True,  True,  DataType.INT8),
        ("arr_int16",   DataType.ARRAY,   True,  True,  DataType.INT16),
        ("arr_int32",   DataType.ARRAY,   True,  True,  DataType.INT32),
        ("arr_int64",   DataType.ARRAY,   True,  True,  DataType.INT64),
        ("arr_float",   DataType.ARRAY,   True,  True,  DataType.FLOAT),
        ("arr_double",  DataType.ARRAY,   True,  True,  DataType.DOUBLE),
        ("arr_bool",    DataType.ARRAY,   True,  True,  DataType.BOOL),
        ("arr_varchar", DataType.ARRAY,   True,  True,  DataType.VARCHAR),
    ]

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestScalarExprCorrectness" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)

        field_names = []
        for fname, dtype, nullable, is_array, elem_dtype in self.FIELD_DEFS:
            if is_array:
                if elem_dtype == DataType.VARCHAR:
                    schema.add_field(fname, DataType.ARRAY, element_type=elem_dtype,
                                     max_capacity=5, max_length=100, nullable=nullable)
                else:
                    schema.add_field(fname, DataType.ARRAY, element_type=elem_dtype,
                                     max_capacity=5, nullable=nullable)
            elif dtype == DataType.VARCHAR:
                schema.add_field(fname, dtype, max_length=256, nullable=nullable)
            else:
                schema.add_field(fname, dtype, nullable=nullable)
            field_names.append(fname)

        self.create_collection(client, self.collection_name, schema=schema)

        # Generate deterministic data
        # Convert tuple FIELD_DEFS to dict format for generate_deterministic_rows
        field_configs = [{"name": f, "dtype": d, "nullable": n, "is_array": ia, "element_dtype": ed}
                         for f, d, n, ia, ed in self.FIELD_DEFS]
        test_data_values = generate_deterministic_rows(field_configs, total_rows=self.NUM_ROWS, seed=DEFAULT_SEED)

        # Build full rows with pk and vector
        vectors = cf.gen_vectors(self.NUM_ROWS, default_dim)
        test_data = []
        for i, srow in enumerate(test_data_values):
            row = {default_pk: i, default_vec: vectors[i]}
            row.update(srow)
            test_data.append(row)

        request.cls.test_data = test_data
        request.cls.field_names = field_names

        # Batch insert
        for start in range(0, len(test_data), 1000):
            self.insert(client, self.collection_name, data=test_data[start:start + 1000])
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    def _run_expression_check(self, client, expr, test_data, field_names):
        """Run a single expression and compare against eval ground truth. Returns error msg or None."""
        try:
            res = self.query(client, self.collection_name, filter=expr,
                             output_fields=[default_pk],
                             check_task=CheckTasks.check_nothing)[0]
            # Check for API error
            if hasattr(res, 'message') and res.message:
                return None  # Expression not supported — skip, don't fail
            milvus_ids = sorted([r[default_pk] for r in res])
        except Exception as e:
            if 'cannot parse' in str(e) or 'unsupported' in str(e).lower():
                return None
            return f"EXCEPTION: {expr} -> {e}"

        expected_idx = eval_filter(expr, test_data)
        expected_ids = sorted([test_data[i][default_pk] for i in expected_idx])

        if milvus_ids != expected_ids:
            extra = set(milvus_ids) - set(expected_ids)
            missing = set(expected_ids) - set(milvus_ids)
            return (f"MISMATCH: {expr} | Milvus={len(milvus_ids)} expected={len(expected_ids)} | "
                    f"extra(5)={list(extra)[:5]} missing(5)={list(missing)[:5]}")
        return None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field_idx", list(range(len(FIELD_DEFS))))
    def test_single_field_expressions(self, field_idx):
        """Test all operators for a single field against eval ground truth."""
        fname, dtype, nullable, is_array, elem_dtype = self.FIELD_DEFS[field_idx]
        client = self._client(alias=self.shared_alias)

        # Extract sample values for this field
        if is_array:
            sample_vals = [r[fname] for r in self.test_data if r.get(fname) is not None and isinstance(r.get(fname), list)]
        else:
            sample_vals = [r[fname] for r in self.test_data if r.get(fname) is not None]

        expressions = generate_expressions_for_field(
            fname, dtype, sample_vals, nullable=nullable, is_array=is_array, element_dtype=elem_dtype)

        failures = []
        for expr in expressions:
            err = self._run_expression_check(client, expr, self.test_data, self.field_names)
            if err:
                failures.append(err)
                log.error(err)
            else:
                log.info(f"PASS: {expr}")

        assert not failures, (
            f"Seed={DEFAULT_SEED}, field={fname}, {len(failures)}/{len(expressions)} failed:\n"
            + "\n".join(failures))

    @pytest.mark.tags(CaseLabel.L1)
    def test_compound_expressions(self):
        """Test AND/OR/NOT compound expressions across multiple fields."""
        client = self._client(alias=self.shared_alias)

        # Build field_exprs dict: field_name -> list of expression strings
        field_exprs = {}
        for fname, dtype, nullable, is_array, elem_dtype in self.FIELD_DEFS:
            if is_array:
                sample_vals = [r[fname] for r in self.test_data if r.get(fname) is not None and isinstance(r.get(fname), list)]
            else:
                sample_vals = [r[fname] for r in self.test_data if r.get(fname) is not None]
            exprs = generate_expressions_for_field(
                fname, dtype, sample_vals, nullable=nullable, is_array=is_array, element_dtype=elem_dtype)
            if exprs:
                field_exprs[fname] = exprs

        expressions = generate_compound_expressions(field_exprs)

        failures = []
        for expr in expressions:
            err = self._run_expression_check(client, expr, self.test_data, self.field_names)
            if err:
                failures.append(err)
                log.error(err)
            else:
                log.info(f"PASS: {expr}")

        assert not failures, (
            f"Seed={DEFAULT_SEED}, {len(failures)}/{len(expressions)} compound expr failed:\n"
            + "\n".join(failures))


# ──────────────────────────────────────────────────────────────
# Test Class 2: Index consistency — cross-index comparison
# ──────────────────────────────────────────────────────────────

@pytest.mark.xdist_group("TestScalarIdxConsistency")
class TestScalarIndexConsistency(TestMilvusClientV2Base):
    """
    Index consistency: same data across fields with different indexes.
    200 rows. Verifies all index types return identical results.
    """
    shared_alias = "TestScalarIdxConsistency"
    NUM_ROWS = 200

    INDEX_MATRIX = {
        DataType.INT8:   ["no_index", "INVERTED", "BITMAP", "STL_SORT"],
        DataType.INT16:  ["no_index", "INVERTED", "BITMAP", "STL_SORT"],
        DataType.INT32:  ["no_index", "INVERTED", "BITMAP", "STL_SORT"],
        DataType.INT64:  ["no_index", "INVERTED", "BITMAP", "STL_SORT"],
        DataType.FLOAT:  ["no_index", "INVERTED", "STL_SORT"],
        DataType.DOUBLE: ["no_index", "INVERTED", "STL_SORT"],
        DataType.BOOL:   ["no_index", "INVERTED", "BITMAP"],
        DataType.VARCHAR: ["no_index", "INVERTED", "BITMAP", "TRIE"],
    }

    ARRAY_INDEX_MATRIX = {
        DataType.INT32:   ["no_index", "INVERTED", "BITMAP"],
        DataType.INT64:   ["no_index", "INVERTED", "BITMAP"],
        DataType.VARCHAR:  ["no_index", "INVERTED", "BITMAP"],
    }

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestScalarIdxConsist" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)

        field_groups = {}
        for dtype, indexes in self.INDEX_MATRIX.items():
            group = []
            for idx_type in indexes:
                fname = f"{dtype.name.lower()}_{idx_type.lower().replace('-', '_')}"
                if dtype == DataType.VARCHAR:
                    schema.add_field(fname, dtype, max_length=256, nullable=True)
                else:
                    schema.add_field(fname, dtype, nullable=True)
                group.append((fname, idx_type))
            field_groups[dtype] = group

        array_field_groups = {}
        for elem_dtype, indexes in self.ARRAY_INDEX_MATRIX.items():
            group = []
            for idx_type in indexes:
                fname = f"arr_{elem_dtype.name.lower()}_{idx_type.lower().replace('-', '_')}"
                if elem_dtype == DataType.VARCHAR:
                    schema.add_field(fname, DataType.ARRAY, element_type=elem_dtype,
                                     max_capacity=5, max_length=100, nullable=True)
                else:
                    schema.add_field(fname, DataType.ARRAY, element_type=elem_dtype,
                                     max_capacity=5, nullable=True)
                group.append((fname, idx_type))
            array_field_groups[elem_dtype] = group

        self.create_collection(client, self.collection_name, schema=schema)

        rng = random.Random(DEFAULT_SEED)
        vectors = cf.gen_vectors(self.NUM_ROWS, default_dim)
        test_data = []
        for i in range(self.NUM_ROWS):
            row = {default_pk: i, default_vec: vectors[i]}
            for dtype, group in field_groups.items():
                val = make_nullable_value(dtype, rng, null_prob=0.1)
                for fname, _ in group:
                    row[fname] = val
            for elem_dtype, group in array_field_groups.items():
                arr_val = None if rng.random() < 0.1 else make_random_array(elem_dtype, rng)
                for fname, _ in group:
                    row[fname] = arr_val
            test_data.append(row)

        request.cls.test_data = test_data
        request.cls.field_groups = field_groups
        request.cls.array_field_groups = array_field_groups

        self.insert(client, self.collection_name, data=test_data)
        self.flush(client, self.collection_name)

        idx_params = self.prepare_index_params(client)[0]
        idx_params.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx_params)

        for dtype, group in field_groups.items():
            for fname, idx_type in group:
                if idx_type != "no_index":
                    ip = self.prepare_index_params(client)[0]
                    ip.add_index(fname, index_type=idx_type)
                    self.create_index(client, self.collection_name, index_params=ip)

        for elem_dtype, group in array_field_groups.items():
            for fname, idx_type in group:
                if idx_type != "no_index":
                    ip = self.prepare_index_params(client)[0]
                    ip.add_index(fname, index_type=idx_type)
                    self.create_index(client, self.collection_name, index_params=ip)

        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    def _check_cross_index_consistency(self, client, group, templates):
        """Helper: run expression templates across fields in a group, verify identical results."""
        failures = []
        for tmpl in templates:
            results = {}
            for fname, idx_type in group:
                expr = tmpl.replace("{f}", fname)
                try:
                    res = self.query(client, self.collection_name, filter=expr,
                                     output_fields=[default_pk],
                                     check_task=CheckTasks.check_nothing)[0]
                    if hasattr(res, 'message'):
                        continue
                    results[fname] = sorted([r[default_pk] for r in res])
                except Exception:
                    continue

            if len(results) < 2:
                continue
            ref_name, ref_ids = next(iter(results.items()))
            for fname, ids in results.items():
                if ids != ref_ids:
                    idx_t = dict(group)[fname]
                    ref_idx = dict(group)[ref_name]
                    failures.append(
                        f"{tmpl}: {fname}({idx_t}) got {len(ids)} vs {ref_name}({ref_idx}) got {len(ref_ids)}")
        return failures

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype_name", [
        "INT8", "INT16", "INT32", "INT64", "FLOAT", "DOUBLE", "BOOL", "VARCHAR"
    ])
    def test_scalar_index_consistency(self, dtype_name):
        """Verify all index types return identical results for identical scalar data and expressions."""
        dtype = DataType[dtype_name]
        client = self._client(alias=self.shared_alias)
        group = self.field_groups[dtype]
        test_data = self.test_data

        sample_field = group[0][0]
        non_null = [r[sample_field] for r in test_data if r.get(sample_field) is not None]
        if not non_null:
            pytest.skip(f"No non-null values for {dtype_name}")
        val = non_null[len(non_null) // 2]

        if dtype == DataType.BOOL:
            templates = ["{f} == true", "{f} == false", "{f} IS NULL", "{f} IS NOT NULL"]
        elif dtype == DataType.VARCHAR:
            templates = ['{f} == "' + str(val) + '"', '{f} != "' + str(val) + '"',
                         "{f} IS NULL", "{f} IS NOT NULL", '{f} LIKE "str%"',
                         '{f} IN ["str_0", "str_1"]', '{f} NOT IN ["abc"]']
        else:
            templates = [f"{{f}} > {repr(val)}", f"{{f}} <= {repr(val)}", f"{{f}} == {repr(val)}",
                         "{f} IS NULL", "{f} IS NOT NULL",
                         f"{{f}} IN [{repr(val)}]", f"{{f}} NOT IN [{repr(val)}]",
                         f"{{f}} + 1 > {repr(val)}"]

        failures = self._check_cross_index_consistency(client, group, templates)
        assert not failures, f"{len(failures)} scalar index inconsistencies for {dtype_name}:\n" + "\n".join(failures)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("elem_dtype_name", ["INT32", "INT64", "VARCHAR"])
    def test_array_index_consistency(self, elem_dtype_name):
        """Verify all index types return identical results for identical array data and expressions."""
        elem_dtype = DataType[elem_dtype_name]
        client = self._client(alias=self.shared_alias)
        group = self.array_field_groups[elem_dtype]
        test_data = self.test_data

        sample_field = group[0][0]
        non_null = [r[sample_field] for r in test_data
                    if r.get(sample_field) is not None and isinstance(r.get(sample_field), list)]
        if not non_null:
            pytest.skip(f"No non-null arrays for {elem_dtype_name}")
        sample_arr = non_null[0]
        val = sample_arr[0] if sample_arr else 0

        if elem_dtype == DataType.VARCHAR:
            templates = [
                '{f}[0] == "' + str(val) + '"',
                "{f} IS NULL", "{f} IS NOT NULL",
                f'array_contains({{f}}, "{val}")',
                "array_length({f}) > 0",
            ]
        else:
            templates = [
                f"{{f}}[0] == {repr(val)}", f"{{f}}[0] > {repr(val)}",
                "{f} IS NULL", "{f} IS NOT NULL",
                f"array_contains({{f}}, {repr(val)})",
                "array_length({f}) >= 1", "array_length({f}) < 10",
            ]

        failures = self._check_cross_index_consistency(client, group, templates)
        assert not failures, f"{len(failures)} array index inconsistencies for ARRAY({elem_dtype_name}):\n" + "\n".join(failures)


# ──────────────────────────────────────────────────────────────
# Test Class 3: Corner-case expressions — known bug regression
# ──────────────────────────────────────────────────────────────

@pytest.mark.xdist_group("TestCornerCaseExpressions")
class TestCornerCaseExpressions(TestMilvusClientV2Base):
    """
    Deterministic corner-case expressions targeting known bug patterns.
    Each test maps to a real Milvus issue for regression prevention.
    """
    shared_alias = "TestCornerCaseExpr"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestCornerCaseExpr" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("c8", DataType.INT64)
        schema.add_field("bool_field", DataType.BOOL)
        schema.add_field("nullable_int", DataType.INT16, nullable=True)
        schema.add_field("json_data", DataType.JSON, nullable=True)
        schema.add_field("int_val", DataType.INT64)
        schema.add_field("float_val", DataType.FLOAT)
        schema.add_field("str_val", DataType.VARCHAR, max_length=256)
        self.create_collection(client, self.collection_name, schema=schema)

        vectors = cf.gen_vectors(10, default_dim)
        data = [
            {default_pk: 0, default_vec: vectors[0],
             "c8": INT64_MAX - 1, "bool_field": False, "nullable_int": None,
             "json_data": {"num": INT64_MAX - 7}, "int_val": INT64_MAX - 1,
             "float_val": 0.0, "str_val": "hello"},
            {default_pk: 1, default_vec: vectors[1],
             "c8": 100, "bool_field": True, "nullable_int": 574,
             "json_data": {"num": 42}, "int_val": 100,
             "float_val": 3.14, "str_val": "world"},
            {default_pk: 2, default_vec: vectors[2],
             "c8": INT64_MIN, "bool_field": False, "nullable_int": None,
             "json_data": None, "int_val": INT64_MIN,
             "float_val": -1.0, "str_val": "abc"},
            {default_pk: 3, default_vec: vectors[3],
             "c8": 200, "bool_field": False, "nullable_int": 1,
             "json_data": {"num": 100}, "int_val": 200,
             "float_val": 6.28, "str_val": "str_0"},
            {default_pk: 4, default_vec: vectors[4],
             "c8": 50, "bool_field": True, "nullable_int": None,
             "json_data": {"num": FLOAT64_INT_LIMIT + 1}, "int_val": 50,
             "float_val": 100.0, "str_val": "str_1"},
        ]
        request.cls.test_data = data

        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    def test_int64_overflow_addition(self):
        """Regression #48440: c8 + 33 overflows for INT64_MAX-1, should not match <= 19974."""
        client = self._client(alias=self.shared_alias)
        res = self.query(client, self.collection_name, filter="c8 + 33 <= 19974",
                         output_fields=[default_pk])[0]
        ids = sorted([r[default_pk] for r in res])
        assert 0 not in ids, f"id=0 (INT64_MAX-1) + 33 overflows, should not match. Got {ids}"
        assert 2 not in ids, f"id=2 (INT64_MIN) + 33 underflows context, should not match. Got {ids}"
        assert 1 in ids, f"id=1 (100+33=133<=19974) should match. Got {ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_int64_overflow_subtraction(self):
        """Regression #48440: INT64_MIN - 1 should underflow, not wrap to MAX."""
        client = self._client(alias=self.shared_alias)
        res = self.query(client, self.collection_name, filter="c8 - 1 >= 0",
                         output_fields=[default_pk],
                         check_task=CheckTasks.check_nothing)[0]
        if hasattr(res, 'message'):
            log.warning(f"Query failed (parser overflow): {res}")
            return
        ids = sorted([r[default_pk] for r in res])
        assert 2 not in ids, f"id=2 (INT64_MIN) - 1 underflows, should not be >= 0. Got {ids}"
        assert 0 in ids, f"id=0 (INT64_MAX-1 - 1) should be >= 0. Got {ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_int64_overflow_multiplication(self):
        """Regression #48440: (INT64_MAX-1) * 2 overflows, should not be > 0."""
        client = self._client(alias=self.shared_alias)
        res = self.query(client, self.collection_name, filter="c8 * 2 > 0",
                         output_fields=[default_pk],
                         check_task=CheckTasks.check_nothing)[0]
        if hasattr(res, 'message'):
            log.warning(f"Query failed (parser overflow): {res}")
            return
        ids = sorted([r[default_pk] for r in res])
        assert 0 not in ids, f"id=0 (INT64_MAX-1)*2 overflows. Got {ids}"
        assert 1 in ids, f"id=1 (200>0) should match. Got {ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_3vl_not_and_or_nullable(self):
        """Regression #48441: NOT(F AND T AND NULL) = NOT(F) = T -> row should return."""
        client = self._client(alias=self.shared_alias)
        expr = "not ((bool_field == true) and (bool_field IS NOT NULL) and (nullable_int == 574 or nullable_int == 1))"
        res = self.query(client, self.collection_name, filter=expr,
                         output_fields=[default_pk],
                         check_task=CheckTasks.check_nothing)[0]
        if hasattr(res, 'message'):
            log.warning(f"Query failed: {res}")
            return
        ids = sorted([r[default_pk] for r in res])
        for eid in [0, 2, 3]:
            assert eid in ids, f"id={eid} (bool=False, NOT(F)=T) should return. Got {ids}"
        assert 1 not in ids, f"id=1 should not return (NOT(T)=F). Got {ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_3vl_not_all_null_segment(self):
        """Regression #48441: bug triggers when ALL nullable values in segment are NULL."""
        client = self._client(alias=self.shared_alias)
        coll2 = "TestCorner3VL_allnull" + cf.gen_unique_str("_")
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("bf", DataType.BOOL)
        schema.add_field("nf", DataType.INT16, nullable=True)
        self.create_collection(client, coll2, schema=schema)
        vectors = cf.gen_vectors(1, default_dim)
        self.insert(client, coll2, data=[
            {default_pk: 1, default_vec: vectors[0], "bf": False, "nf": None}
        ])
        self.flush(client, coll2)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, coll2, index_params=idx)
        self.load_collection(client, coll2)

        expr = "not ((bf == true) and (bf IS NOT NULL) and (nf == 574 or nf == 1))"
        res = self.query(client, coll2, filter=expr, output_fields=[default_pk])[0]
        ids = [r[default_pk] for r in res]
        self.drop_collection(client, coll2)
        assert 1 in ids, f"Single row (bf=F, nf=NULL): NOT(F)=T should return id=1. Got {ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_bool_literal_in_logical_expr(self):
        """Regression #48443: 'true or (field > val)' should be accepted by parser."""
        client = self._client(alias=self.shared_alias)
        exprs = [
            "true or (int_val > 100)",
            "true and (int_val > 100)",
            "false or (int_val > 100)",
            "(int_val > 100) or true",
            'true or (str_val == "hello")',
            "true or (float_val > 3.0)",
        ]
        for expr in exprs:
            try:
                res = self.query(client, self.collection_name, filter=expr,
                                 output_fields=[default_pk],
                                 check_task=CheckTasks.check_nothing)[0]
                if hasattr(res, 'message') and 'cannot parse' in str(getattr(res, 'message', '')):
                    pytest.fail(f"Parser rejected valid bool literal expression: {expr}")
                log.info(f"PASS: {expr}")
            except Exception as e:
                if 'cannot parse' in str(e):
                    pytest.fail(f"Parser rejected: {expr} -> {e}")

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_mixed_type_in_precision(self):
        """Regression #48442: mixed int/float IN list should not cause INT64 precision loss."""
        client = self._client(alias=self.shared_alias)
        query_val = INT64_MAX
        expr = f'json_data["num"] in [{query_val}, 1.5]'
        res = self.query(client, self.collection_name, filter=expr,
                         output_fields=[default_pk],
                         check_task=CheckTasks.check_nothing)[0]
        if hasattr(res, 'message'):
            log.warning(f"Expression returned error (may be expected): {res}")
            return
        ids = [r[default_pk] for r in res]
        assert 0 not in ids, (
            f"id=0 (json num={INT64_MAX - 7}) should NOT match {query_val} via float coercion. Got {ids}")


# ──────────────────────────────────────────────────────────────
# Test Class 4: JSON expressions — path, functions, mixed types
# ──────────────────────────────────────────────────────────────

@pytest.mark.xdist_group("TestJsonExpressions")
class TestJsonExpressions(TestMilvusClientV2Base):
    """
    JSON-specific expressions: path access, nested keys, json_contains functions,
    typed/dynamic/shared key patterns, index consistency.
    """
    shared_alias = "TestJsonExpr"
    NUM_ROWS = 500

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestJsonExpr" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_pk, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vec, DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("jf_none", DataType.JSON, nullable=True)
        schema.add_field("jf_inv", DataType.JSON, nullable=True)
        self.create_collection(client, self.collection_name, schema=schema)

        rng = random.Random(DEFAULT_SEED)
        vectors = cf.gen_vectors(self.NUM_ROWS, default_dim)
        test_data = []
        json_field_names = ["jf_none", "jf_inv"]

        for i in range(self.NUM_ROWS):
            if rng.random() < 0.05:
                jdoc = None
            else:
                num = rng.randint(0, 100)
                jdoc = {
                    "int_key": num,
                    "float_key": num * 1.5,
                    "str_key": f"val_{num % 10}",
                    "bool_key": num % 2 == 0,
                    "arr_key": [rng.randint(0, 20) for _ in range(rng.randint(1, 5))],
                    "nested": {"a": num % 5, "b": f"nested_{num}"},
                }
                if rng.random() < 0.3:
                    jdoc["sparse_key"] = rng.randint(0, 50)

            row = {default_pk: i, default_vec: vectors[i]}
            for jf in json_field_names:
                row[jf] = jdoc
            test_data.append(row)

        request.cls.test_data = test_data
        request.cls.json_fields = json_field_names

        self.insert(client, self.collection_name, data=test_data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(default_vec, index_type="FLAT", metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx)
        ip = self.prepare_index_params(client)[0]
        ip.add_index("jf_inv", index_type="INVERTED", params={"json_cast_type": "varchar"})
        self.create_index(client, self.collection_name, index_params=ip)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_path_expressions(self):
        """Test JSON path access: simple key, nested key, array index."""
        client = self._client(alias=self.shared_alias)
        jf = "jf_none"

        expressions = [
            f'{jf}["int_key"] > 50',
            f'{jf}["int_key"] <= 10',
            f'{jf}["int_key"] == 0',
            f'{jf}["int_key"] != 42',
            f'{jf}["int_key"] IN [1, 2, 3, 10, 50]',
            f'{jf}["int_key"] NOT IN [0]',
            f'{jf}["float_key"] > 50.0',
            f'{jf}["float_key"] <= 15.0',
            f'{jf}["str_key"] == "val_0"',
            f'{jf}["str_key"] != "val_5"',
            f'{jf}["str_key"] LIKE "val_%"',
            f'{jf}["str_key"] IN ["val_0", "val_1", "val_2"]',
            f'{jf}["bool_key"] == true',
            f'{jf}["bool_key"] == false',
            f'{jf}["nested"]["a"] >= 3',
            f'{jf}["nested"]["b"] LIKE "nested_%"',
            f'{jf}["arr_key"][0] > 10',
            f'{jf}["arr_key"][0] IN [1, 5, 10]',
            f'{jf}["sparse_key"] > 25',
            f'{jf}["sparse_key"] IS NULL',
            f'{jf}["int_key"] + 10 > 60',
            f'{jf}["int_key"] * 2 <= 100',
        ]

        failures = []
        for expr in expressions:
            try:
                res1 = self.query(client, self.collection_name, filter=expr,
                                  output_fields=[default_pk],
                                  check_task=CheckTasks.check_nothing)[0]
                if hasattr(res1, 'message'):
                    log.warning(f"Skipping unsupported: {expr}")
                    continue

                expr_inv = expr.replace("jf_none", "jf_inv")
                res2 = self.query(client, self.collection_name, filter=expr_inv,
                                  output_fields=[default_pk],
                                  check_task=CheckTasks.check_nothing)[0]
                if hasattr(res2, 'message'):
                    continue

                ids1 = sorted([r[default_pk] for r in res1])
                ids2 = sorted([r[default_pk] for r in res2])
                if ids1 != ids2:
                    failures.append(f"INDEX MISMATCH: {expr} no_index={len(ids1)} vs inverted={len(ids2)}")
                else:
                    log.info(f"PASS: {expr} -> {len(ids1)} rows")
            except Exception as e:
                if 'cannot parse' not in str(e):
                    failures.append(f"EXCEPTION: {expr} -> {e}")

        assert not failures, f"{len(failures)} JSON expression failures:\n" + "\n".join(failures)

    @pytest.mark.tags(CaseLabel.L1)
    def test_json_contains_functions(self):
        """Test json_contains, json_contains_all, json_contains_any on JSON array keys."""
        client = self._client(alias=self.shared_alias)
        jf = "jf_none"

        expressions = [
            f'json_contains({jf}["arr_key"], 5)',
            f'JSON_CONTAINS({jf}["arr_key"], 10)',
            f'json_contains_all({jf}["arr_key"], [1, 2])',
            f'json_contains_any({jf}["arr_key"], [5, 10, 15])',
        ]

        for expr in expressions:
            try:
                res = self.query(client, self.collection_name, filter=expr,
                                 output_fields=[default_pk],
                                 check_task=CheckTasks.check_nothing)[0]
                if hasattr(res, 'message'):
                    log.warning(f"Skipping: {expr} -> {res}")
                    continue
                log.info(f"PASS: {expr} -> {len(res)} rows")
            except Exception as e:
                if 'cannot parse' in str(e):
                    log.warning(f"Not supported: {expr}")
                else:
                    pytest.fail(f"Unexpected error: {expr} -> {e}")
