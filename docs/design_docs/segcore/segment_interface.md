# Segment Interface

## External Interface
1. `get_row_count`: Get the number of entities in the segment
2. `get_schema`: Get the corresponding collection schema in the segment
3. `GetMemoryUsageInBytes`: Get memory usage of a segment
4. `Search(plan, placeholderGroup, timestamp) -> QueryResult`:  Perform search operations according to the plan containing search parameters and predicate condition, and return search results. Ensure that the time of all search result is before specified timestamp(MVCC)
5. `FillTargetEntry(plan, &queryResult)`: Fill the missing column data for search results based on target columns in the plan

See design details `${milvus_root}/internal/core/src/segcore/SegmentInterface.h`

## Basic Concepts：
1. Segment: Data sharded into segment based on written timestamp, and the sharding logic is controlled by data coordinator.
2. Chunk: further division of segment data, chunk is continuous data for each column
    * There will be only one chunk in each sealed segment.
    * In growing segment, chunks are currently divided by a fixed number of rows. With data ingestion, the number of chunks will increase
3. Span: Similar to std::span, point to continuous data in memory
4. SystemField: Extra field stores system info, currently including RowID and Timestamp field.
5. SegOffset: The entity identifier in the segment

## SegmentInternalInterface internal functions
1. `num_chunk()`: total chunk number
2. `size_per_chunk()`: length of each chunk
3. `get_active_count(Timestamp)`: entity count after filter by Timestamp
4. `chunk_data(FieldOffset, chunk_id) -> Span<T>`: return continuous data for specified column and chunk
5. `chunk_scalar_index(FieldOffset, chunk_id) -> const StructuredIndex<T>&`: return the inverted index of specified column and chunk
6. `num_chunk_index`: the number of indexes (including scalar and vector indexes) that have been created:
    1. In growing segment, this value is the number of chunks for which the inverted index has been created. In these chunks, the index can be used to speed up the calculation.
    2. SealedSegment must be 1
7. `debug()`: debug is used to print extra information while debugging
8. `vector_search (vec_count, query..., timestamp, bitset, output)`: Search the vector column
    1. `vec_count`: specifies how many entities participated in the vector search calculation, rest of the segments are filtered out because it's timestamp is larger than specified timestamp. This function is mainly used in growing segment as multi version control(MVCC)
    2. `query...`: multiple variables jointly specify the parameters and search vector
    3. `timestamp`: timestamp is used for time travelling, filter out data with timestamp. Mainly for sealed segment
    4. `bitset`: calculated bit mask value as a output
    5. `output`: output QueryResult
9. `bulk_subscript(FieldOffset|SystemField, seg_offsets..., output)`:
    - given seg_offsets, calculate `results[i] = FieldData[seg_offsets[i]]`, for GetEntityByIds
    - FieldData is defined by FieldOffset or SystemField
10. `search_ids(IdArray, timestamp) -> pair<IdArray, SegOffsets>`:
    1.  Find the corresponding segoffset according to the primary key in idarray
    2.  The returned order is not guaranteed, but the two returned fields must correspond to each other one by one.
    3.  Entities without PKs will not be returned
11. `check_search(Plan)`: check if the Plan is valid
    1.  It mainly checks whether the columns used in the plan have been loaded
