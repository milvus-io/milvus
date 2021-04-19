#ifdef __cplusplus
extern "C" {
#endif

typedef void* CCollection;

CCollection
NewCollection(const char* collection_name, const char* schema_conf);

void
DeleteCollection(CCollection collection);

#ifdef __cplusplus
}
#endif