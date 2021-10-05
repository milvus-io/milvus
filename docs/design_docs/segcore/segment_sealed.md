# SegmentSealed
SegmentSealed has extra interface rather than segment_inferface:

1. `LoadIndex(loadIndexInfo)`: load the index. indexInfo containts
    1. `FieldId`
    2. `IndexParams`: index paramters in KV structure KV 
    3. `VecIndex`: vector index
2. `LoadFieldData(loadFieldDataInfo)`: Load column data, could be either scalar column or vector column
    1. Note: indexes and vector data for the same column may coexist. Indexes are prioritized in search 
3. `DropIndex(fieldId)`: drop and release exist index of specified field 
4. `DropFieldData(fieldId)`: drop and release exist data for specified field

Search is executatble as long as all the column involved in the search are loaded.