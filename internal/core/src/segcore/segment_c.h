#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include "collection_c.h"
#include "plan_c.h"

typedef void* CSegmentBase;

CSegmentBase
NewSegment(CCollection collection, unsigned long segment_id);

void
DeleteSegment(CSegmentBase segment);

//////////////////////////////////////////////////////////////////

int
Insert(CSegmentBase c_segment,
       long int reserved_offset,
       signed long int size,
       const long* row_ids,
       const unsigned long* timestamps,
       void* raw_data,
       int sizeof_per_row,
       signed long int count);

long int
PreInsert(CSegmentBase c_segment, long int size);

int
Delete(
    CSegmentBase c_segment, long int reserved_offset, long size, const long* row_ids, const unsigned long* timestamps);

long int
PreDelete(CSegmentBase c_segment, long int size);

int
Search(CSegmentBase c_segment,
       CPlan plan,
       CPlaceholderGroup* placeholder_groups,
       unsigned long* timestamps,
       int num_groups,
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