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
