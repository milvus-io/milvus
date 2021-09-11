# What's Knowhere

## Concepts
Vector index is a time-efficient and space-efficient data structure built on vectors through a certain mathematical model. Through the vector index, we can efficiently query several vectors similar to the target vector.
Since accurate retrieval is usually very time-consuming, most of the vector index types of Milvus use ANNS (Approximate Nearest Neighbors Search). Compared with accurate retrieval, the core idea of ANNS is no longer limited to returning the most accurate result, but only searching for neighbors of the target. ANNS improves retrieval efficiency by sacrificing accuracy within an acceptable range.

## What can knowhere do
Knowhere is the vector search execution engine of Milvus. It encapsulates many popular vector index algorithm libraries, such as faiss, hnswlib, NGT, annoy, and provides a set of unified interfaces. In addition, Knowhere also supports heterogeneous computing.

## Framework
![Knowhere framework](graphs/knowhere_framework.png)

For more index types and heterogeneous support, please refer to the vector index document.

## Major Interface
``` C++
/*
 * Serialize
 * @return: serialization data
 */
BinarySet
Serialize();

/*
 * Load from serialization data
 * @param dataset_ptr: serialization data
 */
void
Load(const BinarySet&);

/*
 * Create index
 * @param dataset_ptr: index data (key of the Dataset is "tensor", "rows" and "dim")
 * @parma config: index param
 */
void
BuildAll(const DatasetPtr& dataset_ptr, const Config& config);

/*
 * KNN (K-Nearest Neighbors) Query
 * @param dataset_ptr: query data (key of the Dataset is "tensor" and "rows")
 * @parma config: query param
 * @parma blacklist: mark for deletion
 * @return: query result (key of the Dataset is "ids" and "distance")
 */
DatasetPtr
Query(const DatasetPtr& dataset_ptr, const Config& config, BitsetView blacklist);

/*
 * Copy the index from GPU to CPU
 * @return: CPU vector index
 * @notes: Only valid of the GPU indexes
 */
VecIndexPtr
CopyGpuToCpu();

/*
 * If the user IDs has been set, they will be returned in the Query interface;
 * else the range of the returned IDs is [0, row_num-1].
 * @parma uids: user ids
 */
void
SetUids(std::shared_ptr<std::vector<IDType>> uids);

/*
 * Get the size of the index in memory.
 * @return: index memory size
 */
int64_t
Size();
```

## Data Format
The vector data used for index and query is stored as a one-dimensional array.
And the first `dim * sizeof(data_type)` bytes of the array is the first vector; then `row_num -1` vectors followed.


## Sequence
### Create index
![create index sequence](graphs/create_index.png)

### Query
![knn query sequence](graphs/knn_query.png)
