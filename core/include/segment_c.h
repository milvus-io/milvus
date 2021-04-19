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

unsigned long
GetTimeBegin(CSegmentBase c_segment);

void
SetTimeBegin(CSegmentBase c_segment, unsigned long time_begin);

unsigned long
GetTimeEnd(CSegmentBase c_segment);

void
SetTimeEnd(CSegmentBase c_segment, unsigned long time_end);

unsigned long
GetSegmentId(CSegmentBase c_segment);

void
SetSegmentId(CSegmentBase c_segment, unsigned long segment_id);

#ifdef __cplusplus
}
#endif