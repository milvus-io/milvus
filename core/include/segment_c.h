#ifdef __cplusplus
extern "C" {
#endif

#include "partition_c.h"

typedef void* CSegmentBase;

CSegmentBase NewSegment(CPartition partition, unsigned long segment_id);

void DeleteSegment(CSegmentBase segment);

int Insert(CSegmentBase c_segment,
                signed long int size,
                const unsigned long* primary_keys,
                const unsigned long int* timestamps,
                void* raw_data,
                int sizeof_per_row,
                signed long int count);

#ifdef __cplusplus
}
#endif