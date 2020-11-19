#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>

int
MergeInto(
    long int num_queries, long int topk, float* distances, long int* uids, float* new_distances, long int* new_uids);

#ifdef __cplusplus
}
#endif
