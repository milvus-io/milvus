#ifdef __cplusplus
extern "C" {
#endif

//struct DogDataChunk {
//    void* raw_data;      // schema
//    int sizeof_per_row;  // alignment
//    signed long int count;
//};

typedef void* CSegmentBase;

CSegmentBase SegmentBaseInit();

//int32_t Insert(CSegmentBase c_segment, signed long int size, const unsigned long* primary_keys, const unsigned long int* timestamps, DogDataChunk values);

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