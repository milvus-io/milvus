aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/../pb PB_SRC_FILES)

# add_definitions(-DBOOST_STACKTRACE_USE_ADDR2LINE)
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
        ${PB_SRC_FILES}
        )
add_library(milvus_dog_segment SHARED
        ${DOG_SEGMENT_FILES}
        )


#add_dependencies( segment sqlite mysqlpp )

target_link_libraries(milvus_dog_segment
        tbb utils pthread knowhere log libprotobuf
        dl backtrace
        milvus_query
        )

