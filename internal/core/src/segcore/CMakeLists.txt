
set(SEGCORE_FILES
        SegmentNaive.cpp
        SegmentSmallIndex.cpp
        IndexMeta.cpp
        Collection.cpp
        collection_c.cpp
        segment_c.cpp
        SegmentBase.cpp
        IndexingEntry.cpp
        InsertRecord.cpp
        Reduce.cpp
        plan_c.cpp
        reduce_c.cpp
        load_index_c.cpp)
add_library(milvus_segcore SHARED
        ${SEGCORE_FILES}
        )

target_link_libraries(milvus_segcore
        tbb milvus_utils pthread knowhere log milvus_proto
        dl backtrace
        milvus_common
        milvus_query
        )

