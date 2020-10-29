
set(DOG_SEGMENT_FILES
        SegmentNaive.cpp
        SegmentSmallIndex.cpp
        IndexMeta.cpp
        ConcurrentVector.cpp
        Collection.cpp
        Partition.cpp
        collection_c.cpp
        partition_c.cpp
        segment_c.cpp
        EasyAssert.cpp
        SegmentBase.cpp
        )
add_library(milvus_dog_segment SHARED
        ${DOG_SEGMENT_FILES}
        )

target_link_libraries(milvus_dog_segment
        tbb utils pthread knowhere log milvus_proto
        dl backtrace
        milvus_query
        )

