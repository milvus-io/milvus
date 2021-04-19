set(DOG_SEGMENT_FILES
        SegmentNaive.cpp
        Collection.cpp
        segment_c.cpp
        )
# Third Party dablooms file
#aux_source_directory( ${MILVUS_THIRDPARTY_SRC}/dablooms  THIRDPARTY_DABLOOMS_FILES )
add_library(milvus_dog_segment SHARED
    ${DOG_SEGMENT_FILES}
)
#add_dependencies( segment sqlite mysqlpp )

target_link_libraries(milvus_dog_segment tbb milvus_utils pthread)
