set(DOG_SEGMENT_FILES
        SegmentNaive.cpp
        IndexMeta.cpp
        ConcurrentVector.cpp
        # Collection.cpp
        # Partition.cpp
        # collection_c.cpp
        # partition_c.cpp
        # segment_c.cpp
        )
add_library(milvus_dog_segment SHARED
    ${DOG_SEGMENT_FILES}
)
#add_dependencies( segment sqlite mysqlpp )

target_link_libraries(milvus_dog_segment tbb milvus_utils pthread)

