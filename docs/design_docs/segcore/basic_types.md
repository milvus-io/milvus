# Glossary

- `Collection`: Data table containing multiple Segments
- `Segment`: The memory structure of storing a piece of data which supports concurrent insertion, deletion, query, index loading, monitoring and statistics
- `Schema`: Definition of collection data format, including
  - `vector<FieldMeta>`: Order list of FieldMeta
  - `isAutoId`: if set to True , default primary field is `RowId` and it is auto generated
  - `primaryKey`: (when `isAutoId = False`) specify primary key field
- `FieldMeta`: field properties, including
  - `DataType`: data type, including Int8...Int64, Float, Double, FloatVector, BinaryVector and String later
  - `Dim`: (when dataType is vector) type vector dimension
  - `metric_type`: (when dataType is vector type, optional) the metric type corresponding to this vector is related to the small batch index and can be empty
  - `FieldName`: column name
  - `FieldId`: unique number of the column
  - (hidden) `FieldOffset`: which is the subscript of `vector<Field>` in the schema. The internal calculation of segcore is basically based on fieldoffset
- `Span`: similar to STD::span. It supports vector type of data and can be implicitly converted to `SpanBase` for interface overwrite
