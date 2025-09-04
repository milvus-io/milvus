# Expression Filtering Tests

This directory contains comprehensive test modules for Milvus client expression filtering capabilities.

## Test Modules

### 1. `test_milvus_client_scalar_expression_filtering_optimized.py`
**Primary test module for comprehensive scalar expression filtering**

**Features:**
- Tests all Milvus-supported scalar data types (INT8, INT16, INT32, INT64, BOOL, FLOAT, DOUBLE, VARCHAR, ARRAY, JSON)
- Covers all operators: Comparison (==, !=, >, <, >=, <=), Range (IN, LIKE), Arithmetic (+, -, *, /, %, **), Logical (AND, OR, NOT), Null (IS NULL, IS NOT NULL)
- Single collection design with multiple index types for efficiency
- Index consistency verification (same results for indexed vs non-indexed fields)
- Comprehensive error handling and failure debugging
- Automatic reproduction script generation
- Test complex Json expression (JSON[JSON], JSON[LIST[JSON]], JSON[JSON[LIST]], etc)

**Key Design:**
- One collection containing all data types
- Each data type has multiple fields representing different index types
- 10% of data is NULL to test IS NULL/IS NOT NULL operators
- Specific VARCHAR patterns: `str_xxx`, `xxx_str`, `xxx_str_xxx`
- Comprehensive LIKE pattern coverage with escape handling
- Create examples of typed, dynamic, and shared keys in json
- Generate expressions to valida query result

### 2. `test_milvus_client_scalar_expression_filtering.py`
**Legacy comprehensive scalar expression filtering test**

**Features:**
- Original comprehensive test implementation
- Multiple collection approach
- Extensive test coverage for all data types and operators
- Detailed validation logic

### 3. `test_milvus_client_random_expression_generator.py`
**Random expression generation for edge case testing**

**Features:**
- Generates random complex expressions
- Tests edge cases and unusual combinations
- Stress testing for expression parsing
- Random data generation with various patterns

## Data Type Coverage

### Supported Scalar Types
- **Numeric**: INT8, INT16, INT32, INT64, FLOAT, DOUBLE
- **Boolean**: BOOL
- **String**: VARCHAR
- **Array**: ARRAY (with all element types)
- **JSON**: JSON (with complex nested structures)

### Array Element Types
- All scalar types: INT8, INT16, INT32, INT64, BOOL, FLOAT, DOUBLE, VARCHAR

## Operator Coverage

### Comparison Operators
- `==`, `!=`, `>`, `<`, `>=`, `<=`

### Range Operators
- `IN` (with array indexing support)
- `LIKE` (with comprehensive pattern coverage)

### Arithmetic Operators
- `+`, `-`, `*`, `/`, `%`, `**`

### Logical Operators
- `AND`, `OR`, `NOT`

### Null Operators
- `IS NULL`, `IS NOT NULL`

### Array Functions
- Array indexing: `field[index]`

### JSON Functions
- JSON key access: `field['key']`

## Index Type Support

### Scalar Index Types
| Data Types                                               | INVERTED | BITMAP | STL_SORT | Trie | NGRAM | AUTOINDEX |
|:---------------------------------------------------------|:--------:|:------:|:--------:|:----:|:-----:|:---------:|
| INT8, INT16, INT32, INT64                                |   yes    |  yes   |   yes    |  no  |  no   |    yes    |
| BOOL                                                     |   yes    |  yes   |    no    |  no  |  no   |    yes    |
| FLOAT, DOUBLE                                            |   yes    |   no   |   yes    |  no  |  no   |    yes    |
| VARCHAR                                                  |   yes    |  yes   |    no    | yes  |  yes  |    yes    |
| JSON                                                     |   yes    |   no   |    no    |  no  |  yes* |    yes    |
| ARRAY (elements: BOOL, INT8, INT16, INT32, INT64, VARCHAR) |   yes    |  yes   |    no    |  no  |  no   |    yes    |
| ARRAY (elements: FLOAT, DOUBLE)                          |   yes    |   no   |    no    |  no  |  no   |    yes    |

*JSON fields require `json_path` and `json_cast_type: "varchar"` parameters for NGRAM index

### NGRAM Index Specific Features

The NGRAM index is specialized for efficient text partial matching and fuzzy search on VARCHAR and JSON fields.

**Supported Fields:**
- **VARCHAR**: Direct text content indexing
- **JSON**: Requires `json_path` parameter to specify the JSON field path (e.g., `field_name['key']`)

**Index Parameters:**
- `min_gram`: Minimum n-gram length (required, positive integer)
- `max_gram`: Maximum n-gram length (required, positive integer, ≥ min_gram)
- `json_path`: JSON field path for JSON fields (e.g., `"json_field['body']"`)
- `json_cast_type`: Must be `"varchar"` for JSON fields

**Performance Characteristics:**
- Optimized for LIKE queries with `%` and `_` wildcards
- Two-phase query execution: n-gram filtering + secondary validation
- Query strings shorter than `min_gram` fall back to full table scan
- Supports multilingual text including Chinese, Japanese, and Korean

**Example Index Creation:**
```python
# VARCHAR field
index_params.add_index(
    field_name="content",
    index_type="NGRAM",
    params={"min_gram": 2, "max_gram": 3}
)

# JSON field
index_params.add_index(
    field_name="json_field",
    index_type="NGRAM",
    params={
        "min_gram": 2,
        "max_gram": 3,
        "json_path": "json_field['body']",
        "json_cast_type": "varchar"
    }
)
```

## Test Features

### Error Handling
- Parsing error detection and skipping
- Graceful handling of unsupported expressions
- Detailed error reporting

### Debugging Support
- Automatic debug info saving on failure
- Parquet file export for test data
- Reproduction script generation
- Schema and configuration preservation

### Validation Logic
- Ground truth calculation using Python lambdas
- Result count and ID verification
- Index consistency verification

### LIKE Pattern Coverage
- Prefix patterns: `str%`
- Suffix patterns: `%str`
- Contains patterns: `%str%`
- Single character wildcard: `str_`, `_str`
- Combination patterns: `str_%`, `%_str`
- Escape patterns: `str\%`, `str\_`

**NGRAM Index Optimization:**
- LIKE queries on VARCHAR and JSON fields with NGRAM index are automatically optimized
- Query performance significantly improves for pattern matching operations
- Supports all LIKE patterns with `%` and `_` wildcards
- Automatic fallback to full scan when query length < `min_gram`

## Usage

### Running Tests
```bash
# Run optimized test
pytest test_milvus_client_scalar_expression_filtering_optimized.py

# Run legacy comprehensive test
pytest test_milvus_client_scalar_expression_filtering.py

# Run random expression generator
pytest test_milvus_client_random_expression_generator.py

# Run NGRAM index specific tests
pytest ../../testcases/indexes/test_ngram.py
```

### Debug Information
On test failure, debug information is automatically saved to `/tmp/ci_logs/`:
- Test data as Parquet files
- Collection schema and configuration
- Failed expressions list
- Reproduction script

### Reproduction Script
The generated reproduction script can:
- Rebuild the entire test environment
- Recreate schema, data, and indexes
- Re-run failed expressions
- Validate results

## Design Principles

1. **Comprehensive Coverage**: Test all supported data types, operators, and index types (including NGRAM)
2. **Efficiency**: Single collection design for optimal performance
3. **Reliability**: Robust error handling and debugging
4. **Maintainability**: Clear code structure and documentation
5. **Reproducibility**: Automatic failure reproduction capabilities
6. **Index Optimization**: Validate performance improvements with specialized indexes like NGRAM
