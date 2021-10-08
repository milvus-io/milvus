# SegmentSealed
SegmentSealed has extra interface rather than segment_inferface:

1. `LoadIndex(loadIndexInfo)`: load the index. indexInfo contains
    1. `FieldId`
    2. `IndexParams`: index paramters in KV structure KV 
    3. `VecIndex`: vector index
2. `LoadFieldData(loadFieldDataInfo)`: Load column data, could be either scalar column or vector column
    1. Note: indexes and vector data for the same column may coexist. Indexes are prioritized in search 
3. `DropIndex(fieldId)`: drop and release exist index of specified field 
4. `DropFieldData(fieldId)`: drop and release exist data for specified field

Search is executatble as long as all the column involved in the search are loaded.

# SegmentSealedImpl internal data definition
1. `row_count_opt_`:
   1. Fill row count when load the first entity
   2. All the other column loaded must match the same row count
3. `xxx_ready_bitset_` `system_ready_count_`
   1. Used to record whether the corresponding column is loaded. Bitset corresponds to FieldOffset
   2. Query is executatble If and only if the following conditions are met:
      1. system_ready_count_ == 2， which means all the system column RowId/Timestamp is loaded
      2. The scalar columns involved in the query has been loaded
      3. For the vector columns involved in the query, either the original data or the index is loaded
4. `scalar_indexings_`: Store scalar index

   1. Use StructuredSortedIndex in knowhere 
5. `primary_key_index_`: store index for pk column
   1. Use brand new ScalarIndexBase format
   2. **Note，The functions here may overlap with scalar indexes. It is recommended to replace scalar index with ScalarIndexBase**
6. `field_datas_`: store original data
   1. `aligned_vector<char>` format ganruatee `int/float` data are aligned
7. `SealedIndexingRecord vecindexs_`: store vector index
8. `row_ids_/timestamps_`: RowId/Timestamp data
9. `TimestampIndex`: Index for Timestamp column
10. `schema`: schema

# SegmentSealedImpl internal function dafinion
1.Most functions are the implementation of the corresponding functions of segment interface, which will not be repeated here
2. `update_row_count`: Used to update the row_count field
3. `mask_with_timestamps`: Use Timestamp column to update search bitmask，used to support Time Travel function