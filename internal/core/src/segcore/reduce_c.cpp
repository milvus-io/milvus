#include "reduce_c.h"
#include "Reduce.h"

int
MergeInto(
    long int num_queries, long int topk, float* distances, long int* uids, float* new_distances, long int* new_uids) {
    auto status = milvus::segcore::merge_into(num_queries, topk, distances, uids, new_distances, new_uids);
    return status.code();
}
