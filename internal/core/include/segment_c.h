#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include "partition_c.h"

typedef void* CSegmentBase;

typedef struct CQueryInfo {
    long int num_queries;
    int topK;
    const char* field_name;
} CQueryInfo;

CSegmentBase
NewSegment(CPartition partition, unsigned long segment_id);

void
DeleteSegment(CSegmentBase segment);

//////////////////////////////////////////////////////////////////

int
Insert(CSegmentBase c_segment,
           long int reserved_offset,
           signed long int size,
           const long* primary_keys,
           const unsigned long* timestamps,
           void* raw_data,
           int sizeof_per_row,
           signed long int count);

long int
PreInsert(CSegmentBase c_segment, long int size);

int
Delete(CSegmentBase c_segment,
           long int reserved_offset,
           long size,
           const long* primary_keys,
           const unsigned long* timestamps);

long int
PreDelete(CSegmentBase c_segment, long int size);

//int
//Search(CSegmentBase c_segment,
//           const char*  query_json,
//           unsigned long timestamp,
//           float* query_raw_data,
//           int num_of_query_raw_data,
//           long int* result_ids,
//           float* result_distances);

int
Search(CSegmentBase c_segment,
       CQueryInfo  c_query_info,
       unsigned long timestamp,
       float* query_raw_data,
       int num_of_query_raw_data,
       long int* result_ids,
       float* result_distances);

//////////////////////////////////////////////////////////////////

int
Close(CSegmentBase c_segment);

int
BuildIndex(CCollection c_collection, CSegmentBase c_segment);

bool
IsOpened(CSegmentBase c_segment);

long int
GetMemoryUsageInBytes(CSegmentBase c_segment);

//////////////////////////////////////////////////////////////////

long int
GetRowCount(CSegmentBase c_segment);

long int
GetDeletedCount(CSegmentBase c_segment);

#ifdef __cplusplus
}
#endif