# Glossary

- `Collection`: Data table containing multiple Segments.
- `Segment`: The memory structure of storing a piece of data which supports concurrent insertion, deletion, query, index loading, monitoring and statistics.
- `Schema`: Definition of collection data format, including
  - `vector<FieldMeta>`: Order list of FieldMeta.
  - `isAutoId`: If set to True , the default primary field is `RowId` and it is auto-generated.
  - `primaryKey`: (When `isAutoId = False`) Specify primary key field.
- `FieldMeta`: Field properties, including
  - `DataType`: Data type, including Int8...Int64, Float, Double, FloatVector, BinaryVector and String later.
  - `Dim`: (When dataType is vector) Type vector dimension.
  - `metric_type`: (When dataType is vector type, optional) The metric type corresponding to this vector is related to the small-batch index and can be empty.
  - `FieldName`: Column name.
  - `FieldId`: Unique number of the column.
  - (hidden) `FieldOffset`: Which is the subscript of `vector<Field>` in the schema. The internal calculation of segcore is basically based on field offset.
- `Span`: Similar to STD::span. It supports vector type of data and can be implicitly converted to `SpanBase` for interface overwrite.
