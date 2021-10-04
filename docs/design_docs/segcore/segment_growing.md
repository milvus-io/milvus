# SegmentGrowing
Growing segment has the following additional interfaces:

1. `PreInsert(size) -> reseveredOffset`: serial interface, which reserves space for future insertion and returns the `reseveredOffset`.

2. `Insert(reseveredOffset, size, ...Data...)`: write `...Data...` into range `[reseveredOffset, reseveredOffset + size)`. this interface is allowed to be called concurrently.
    
    1. `...Data...` contains row_ids, timestamps two system attributes, and other columns
    2. data column can be stored either row based or columne based.
    3. `PreDelete & Delete(reseveredOffset, row_ids, timestamps)` is the delete interface similar to intert interface.

Growing segment stores data in the form of chunk. The number of rows in each chunk is restricted by configs.

Rows per segment is controlled by parameters `size_per_Chunk ` config

When insert, first allocate enough space to ensure  `total_size <= num_chunk * size_per_chunk`, and then convert data from row format to column format.

During search, each 'chunk' will be searched, and the search results will be saved as 'subquery result',  then reduced into TopK.

Growing Segment also implements small batch index for vectors. The parameters of small batch index are preset in `segcore config`

When `metric type` is specified in the schema, the default parameters will build index for each chunk to accelerate query

## SegmentGrowingImpl internal 

1. SegcoreConfig: contains parameters for Segcore，it has to be speficied before create segment 
2. InsertRecord: inserted data put to here 
3. DeleteRecord: wait for delete implementation
4. IndexingRecord: contains data with small index 
5. SealedIndexing: Record not used any more

### SegcoreConfig
1. Manage chunk_sizeand small index parateters
2. `parse_from` can parse from yaml files（this function is not enabled by default）
   * refer to `${milvus}/internal/core/unittest/test_utils/test_segcore.yaml`
3. `default_config` offers default parameters 

### InsertRecord

Used to manage concurrent inserted data, incluing

1. `atomic<int64_t> reserved`  reserved space calculation
2. `AckResponder` calculate which segment to insert，returns current segment offset
3. `ConcurrentVector` store data columns, each column has one concurrent vector

The following steps are executed when insert,

1. Serially Execute `PreInsert(size) -> reserved_offset` to allocate memory space, the address of space is `[reserved_offset, reserved_offset + size)` is reserved
2. Parallelly execute `Insert(reserved_offset, size, ...Data...)` interface，copy data into the above memory address 

   * First of all，for `ConcurrentVector` of each column, call `grow_to_at_least` to reserve space
   * For each column data, call `set_data_raw` interface to put data into corresponding locations.
   * After execution finished，call`AddSegment` of `AckResponder` ，mark the space `[reserved_offset, reserved_offset + size)` to already inserted

### ConcurrentVector
This is a column data storage that can be inserted concurrently. It is composed of multi data chunks.

1. After`grow_to_at_least(size)` called, reserve space no less than `size` 
2. `set_data_raw(element_offset, source, element_count)` point source to continuous piece of data 
3. `get_span(chunk_id)` get the span of the corresponding chunk