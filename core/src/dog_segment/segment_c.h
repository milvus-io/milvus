#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include "partition_c.h"

typedef void* CSegmentBase;

CSegmentBase
NewSegment(CPartition partition, unsigned long segment_id);

void
DeleteSegment(CSegmentBase segment);

//////////////////////////////////////////////////////////////////

int
Insert(CSegmentBase c_segment,
       signed long int size,
       const long* primary_keys,
       const unsigned long* timestamps,
       void* raw_data,
       int sizeof_per_row,
       signed long int count,
       unsigned long timestamp_min,
       unsigned long timestamp_max);

int
Delete(CSegmentBase c_segment,
       long size,
       const long* primary_keys,
       const unsigned long* timestamps,
       unsigned long timestamp_min,
       unsigned long timestamp_max);

int
Search(CSegmentBase c_segment,
           void* fake_query,
           unsigned long timestamp,
           long int* result_ids,
           float* result_distances);

//////////////////////////////////////////////////////////////////

int
Close(CSegmentBase c_segment);

bool
IsOpened(CSegmentBase c_segment);

//////////////////////////////////////////////////////////////////

long int
GetRowCount(CSegmentBase c_segment);

long int
GetDeletedCount(CSegmentBase c_segment);

#ifdef __cplusplus
}
#endif