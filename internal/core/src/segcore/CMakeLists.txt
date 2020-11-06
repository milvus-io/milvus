
set(SEGCORE_FILES
        SegmentNaive.cpp
        SegmentSmallIndex.cpp
        IndexMeta.cpp
        ConcurrentVector.cpp
        Collection.cpp
        Partition.cpp
        collection_c.cpp
        partition_c.cpp
        segment_c.cpp
        SegmentBase.cpp
        IndexingEntry.cpp
        InsertRecord.cpp
        )
add_library(milvus_segcore SHARED
        ${SEGCORE_FILES}
        )

target_link_libraries(milvus_segcore
        tbb utils pthread knowhere log milvus_proto
        dl backtrace
        milvus_query
        )

