#ifdef __cplusplus
extern "C" {
#endif

typedef void* CCollection;

CCollection
NewCollection(const char* collection_proto);

void
DeleteCollection(CCollection collection);

#ifdef __cplusplus
}
#endif