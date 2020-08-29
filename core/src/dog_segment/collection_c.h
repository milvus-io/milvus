#ifdef __cplusplus
extern "C" {
#endif

typedef void* CCollection;

CCollection NewCollection(const char* collection_name);

void DeleteCollection(CCollection collection);

#ifdef __cplusplus
}
#endif