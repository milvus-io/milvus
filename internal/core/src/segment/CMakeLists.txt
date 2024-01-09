set(MILVUS_SEGMENT_SRCS
      SegmentGrowingImpl.cpp  
      SegmentSealedImpl.cpp
      SegmentInterface.cpp
      SearchBruteForce.cpp
      SearchOnGrowing.cpp
      SearchOnIndex.cpp
      SearchOnSealed.cpp 
      SubSearchResult.cpp
    )
add_library(milvus_segment SHARED ${MILVUS_SEGMENT_SRCS})
if(USE_DYNAMIC_SIMD)
        target_link_libraries(milvus_segment milvus_base milvus_index milvus_simd)
else()
        target_link_libraries(milvus_segment milvus_base milvus_index)
endif()

