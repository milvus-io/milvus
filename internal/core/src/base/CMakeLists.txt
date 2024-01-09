set(MILVUS_BASE_SRCS
      Collection.cpp
      InsertRecord.cpp
      Reduce.cpp
      SegcoreConfig.cpp
      IndexConfigGenerator.cpp
      TimestampIndex.cpp
      Utils.cpp
      ConcurrentVector.cpp
      SystemProperty.cpp
      RangeSearchHelper.cpp
      IndexMeta.cpp
      FieldData.cpp
      Schema.cpp
    )
add_library(milvus_base SHARED ${MILVUS_BASE_SRCS})
if(USE_DYNAMIC_SIMD)
        target_link_libraries(milvus_base milvus-storage milvus_common milvus_simd)
else()
        target_link_libraries(milvus_base milvus-storage milvus_common )
endif()
