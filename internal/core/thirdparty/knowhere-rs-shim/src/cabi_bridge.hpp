#pragma once

#include <cstddef>
#include <cstdint>

extern "C" {

enum class CIndexType : int32_t {
    Flat = 0,
    Hnsw = 1,
    SparseInverted = 12,
    SparseWand = 13,
};

enum class CMetricType : int32_t {
    L2 = 0,
    Ip = 1,
    Cosine = 2,
    Hamming = 3,
};

struct CIndexConfig {
    CIndexType index_type;
    CMetricType metric_type;
    size_t dim;
    size_t ef_construction;
    size_t ef_search;
    size_t num_partitions;
    size_t num_centroids;
    size_t reorder_k;
    size_t prq_nsplits;
    size_t prq_msub;
    size_t prq_nbits;
    size_t num_clusters;
    size_t nprobe;
    int32_t data_type;
};

struct CSearchResult {
    int64_t* ids;
    float* distances;
    size_t num_results;
    float elapsed_ms;
};

struct CBinary {
    uint8_t* data;
    int64_t size;
};

struct CBinarySet {
    char** keys;
    CBinary* values;
    size_t count;
};

struct CBitset {
    uint64_t* data;
    size_t len;
    size_t cap_words;
};

void* knowhere_create_index(CIndexConfig config);
void knowhere_free_index(void* index);
int32_t knowhere_train_index(void* index,
                             const float* vectors,
                             size_t count,
                             size_t dim);
int32_t knowhere_add_index(void* index,
                           const float* vectors,
                           const int64_t* ids,
                           size_t count,
                           size_t dim);
CSearchResult* knowhere_search(const void* index,
                               const float* query,
                               size_t count,
                               size_t top_k,
                               size_t dim);
CSearchResult* knowhere_search_with_bitset(const void* index,
                                           const float* query,
                                           size_t count,
                                           size_t top_k,
                                           size_t dim,
                                           const CBitset* bitset);
void knowhere_free_result(CSearchResult* result);
size_t knowhere_get_index_count(const void* index);
size_t knowhere_get_index_dim(const void* index);
size_t knowhere_get_index_size(const void* index);
int32_t knowhere_has_raw_data(const void* index);
CBinarySet* knowhere_serialize_index(const void* index);
int32_t knowhere_deserialize_index(void* index, const CBinarySet* binset);
int32_t knowhere_load_index(void* index, const char* path);
void knowhere_free_binary_set(CBinarySet* binset);

}  // extern "C"
