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

GrGowing Segment also implements small batch index for vectors. The parameters of small batch index are preset in 'segcoreconfig'

When `metric type ` is specified in the schema, the default parameters will build index for each chunk to accelerate query
