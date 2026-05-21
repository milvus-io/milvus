# JSON Storage Design Document

## 1. Data Model Design

### 1.1 Data Layering

#### Dense Part
A set of "core fields" (such as primary keys and commonly used metadata) that are present in most records.

#### Sparse Part
Additional attributes that appear only in some records, potentially involving unstructured or dynamically extended information.

### 1.2 JSON Splitting and Mapping

#### Dense Field Extraction
When parsing JSON, predefined dense fields are extracted and mapped to independent columns in Parquet. A method similar to Parquet Variant Shredding is used to flatten nested data.

#### Sparse Data Preservation
Fields not included in the dense part are stored in a sparse data field. They are serialized using BSON (Binary JSON) format, leveraging its efficient binary representation and rich data type support, with the result stored in a Parquet BINARY type field.

## 2. Storage Strategy

### 2.1 Columnar Storage for Dense Data
- **Schema Definition**: Create independent columns in Parquet for each dense field, explicitly specifying data types (such as numeric, string, list, etc.).
- **Query Performance**: Columnar format is suitable for large data scanning and aggregation operations, improving query efficiency, especially for vectors, indexes, and frequently queried fields.

### 2.2 Row Storage for Sparse Data
- **BSON Storage**:
  - Serialize sparse data as BSON binary format and store it in a single binary column of the Parquet file.
  - BSON format not only compresses more efficiently but also preserves complete data type information of the original data, avoiding numerous null values and file fragmentation issues.

## 3. Parquet Schema Construction
- **Columnar Part**: Build a fixed schema based on dense fields, with each field having a clear data type definition.
- **Row Part**: Define a dedicated field (e.g., `sparse_data`) for storing sparse data, with type set to BINARY, directly storing BSON data.
- **Hybrid Mode**: When writing, dense data is filled into respective columns, and remaining sparse data is serialized as BSON and written to the `sparse_data` field, achieving a balance between query efficiency and storage flexibility.

## 4. Integration and Implementation Considerations

### 4.1 Data Classification Strategy
- **Density Classification**:
  - Classify fields as dense or sparse based on their frequency of occurrence in records (e.g., greater than 30% for dense), while considering data type consistency. If a field has multiple data types, we should treat data types that appear in more than 30% of records as dense fields, with the remaining types stored as sparse fields.
- **Dynamic Extension**:
  - For dynamically extended fields, regardless of frequency, store them in the BSON-formatted sparse part to simplify schema evolution.

### 4.2 Indexing for Sparse Data Access

#### Sparse Column Key Indexing
To accelerate BSON parsing, an inverted index stores BSON keys along with their offsets and sizes or values if they are of numeric type.

##### Value Data Structure Diagram
| Valid | Type  | Row ID | Offset/Value |
|:-----:|:-----:|:------:|:------------:|
| 1bit  | 4bit  | 27bit  | 16 offset, 16bit size |

- **64-bit Structure Breakdown**:
  - **Bit 1 (Valid)**: 1 bit indicating data validity (1 = valid, 0 = invalid).
  - **Bits 2-5 (Type)**: 4 bits representing the data type.
  - **Bits 5-31 (Row ID)**: 27 bits for the row ID, uniquely identifying the data row.
  - **Bits 32-64 (Last 32 bits)**:
    - If **Valid = 1**: Last 32 bits store the actual data value.
    - If **Valid = 0**: Last 32 bits are split into:
      - **First 16 bits (Offset)**: Indicates the data offset position.
      - **Last 16 bits (Size)**: Indicates the data size.

The column key index is optional, and can be configured at table creation time or modified later through field properties.

## 5. Example Data

### 5.1 Example JSON Records

```json
[
    {"id": 1, "attr1": "value1", "attr2": 100},
    {"id": 2, "attr1": "value2", "attr3": true},
    {"id": 3, "attr1": "value3", "attr4": "extra", "attr5": 3.14}
]
```

- **Dense Data:**
  - The field `id` is considered dense.
- **Sparse Data:**
  - Record 1: `attr1`, `attr2`
  - Record 2: `attr1`, `attr3`
  - Record 3: `attr1`, `attr4`, `attr5`

### 5.2 Parquet File Storage

#### Schema Representation

| Column Name   | Data Type | Description |
|--------------|-----------|-------------|
| **id**       | int64     | Dense column storing the integer identifier. |
| **sparse_data** | binary  | Sparse column storing BSON-serialized data of all remaining fields. |
| **sparse_index** | binary  | Index column storing key offsets for efficient parsing. |

#### Stored Data Breakdown

- **Dense Column (`id`)**:
  - Row 1: `1`
  - Row 2: `2`
  - Row 3: `3`

- **Sparse Column (`sparse_data`)**:
  - **Row 1:** BSON representation of `{"attr1": "value1", "attr2": 100}`
  - **Row 2:** BSON representation of `{"attr1": "value2", "attr3": true}`
  - **Row 3:** BSON representation of `{"attr1": "value3", "attr4": "extra", "attr5": 3.14}`

- **Sparse Index (`sparse_index`)**:
  - **Row 1:** Index entries mapping `attr1` and `attr2` to their respective positions in `sparse_data`.
  - **Row 2:** Index entries mapping `attr1` and `attr3`.
  - **Row 3:** Index entries mapping `attr1`, `attr4`, and `attr5`.

In an actual system, the sparse data would be serialized using a BSON library (e.g., bsoncxx) for a compact binary format. The example above demonstrates the logical mapping of JSON data to the Parquet storage format.

---

