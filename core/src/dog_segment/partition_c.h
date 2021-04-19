#ifdef __cplusplus
extern "C" {
#endif

#include "collection_c.h"

typedef void* CPartition;

CPartition NewPartition(CCollection collection, const char* partition_name);

void DeletePartition(CPartition partition);

#ifdef __cplusplus
}
#endif