#ifdef __cplusplus
extern "C" {
#endif

#include "partition_c.h"

typedef void* CSegmentBase;

CSegmentBase
NewSegment(CPartition partition, unsigned long segment_id);

void
DeleteSegment(CSegmentBase segment);

int
Insert(CSegmentBase c_segment,
           signed long int size,
           const unsigned long* primary_keys,
           const unsigned long* timestamps,
           void* raw_data,
           int sizeof_per_row,
           signed long int count);

int
Delete(CSegmentBase c_segment,
           long size,
           const unsigned long* primary_keys,
           const unsigned long* timestamps);

int
Search(CSegmentBase c_segment,
           void* fake_query,
           unsigned long timestamp,
           long int* result_ids,
           float* result_distances);

#ifdef __cplusplus
}
#endif