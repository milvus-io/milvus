# SegmentGrowing
Segmentgrowing has the following additional interfaces:
1. `PreInsert(size) -> reseveredOffset`: serial interface, which reserves space for future insertion and returns the insertion `reseveredOffset`.

2. `Insert(reseveredOffset, size, ...Data...)`: write `...Data...` into range `[reseveredOffset, reseveredOffset + size)`. this interface is allowed to be called concurrently.
    
    1. `...Data...` contains row_ids, timestamps two system attributes, and other columns
    2. data column can be stored either row based or columne based.
    3. `PreDelete & Delete(reseveredOffset, row_ids, timestamps)` is the delete interface similar to intert interface.

Growing segment stores data in the form of chunk. The number of rows in each chunk is restricted by configs,

Rows per segment is controlled by parameters `size_per_Chunk ` control

When insert, first allocate enough space to ensure  `total_size <= num_chunk * size_per_chunk`, and then convert data in row format to column format.

During search, each 'chunk' will be searched, and the search results will be saved as 'subquery result',  then merged.

Growing Segment also implements small batch index for vectors. The parameters of small batch index are preset in 'segcoreconfig'

When `metric type ` is specified in the schema, the default parameters will build index for each chunk to accelerate query

## SegmentGrowingImpl internal paramters
1. SegcoreConfig contains parameters for Segcore，it has to be speficied before create segment 
2. InsertRecord inserted data here
3. DeleteRecord wait for implementation
4. IndexingRecord data with small index 
5. SealedIndexingRecord not used any more

### SegcoreConfig
1. Manage chunk_sizeand small index parateters
2. `parse_from` can parse from yaml files（this function is not enabled by default）
   1. refer to `${milvus}/internal/core/unittest/test_utils/test_segcore.yaml`
3. `default_config` offers default parameters 

### InsertRecord
Used to manage concurrent inserted data, incluing
1. `atomic<int64_t> reserved`  reserved space calculation
2. `AckResponder` calculate which segment to insert，returns current inserted data offset
3. `ConcurrentVector` store data columns, each column has one concurrent vector
