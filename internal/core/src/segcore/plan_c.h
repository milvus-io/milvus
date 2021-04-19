#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include "collection_c.h"

typedef void* CPlan;
typedef void* CPlaceholderGroup;

CPlan
CreatePlan(CCollection col, const char* dsl);

CPlaceholderGroup
ParsePlaceholderGroup(CPlan plan, void* placeholder_group_blob, long int blob_size);

long int
GetNumOfQueries(CPlaceholderGroup placeholder_group);

long int
GetTopK(CPlan plan);

void
DeletePlan(CPlan plan);

void
DeletePlaceholderGroup(CPlaceholderGroup placeholder_group);

#ifdef __cplusplus
}
#endif