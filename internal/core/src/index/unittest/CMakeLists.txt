include_directories(${INDEX_SOURCE_DIR}/thirdparty)
include_directories(${INDEX_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${INDEX_SOURCE_DIR}/thirdparty/NGT/lib)
include_directories(${INDEX_SOURCE_DIR}/knowhere)
include_directories(${INDEX_SOURCE_DIR})

set( TEST_LIBS  gtest
                gmock
                gtest_main
                gmock_main
                fiu
                )


################################################################################
#<INSTRUCTIONSET-TEST>
add_executable( test_instructionset test_instructionset.cpp )
target_link_libraries( test_instructionset PRIVATE ${TEST_LIBS} )

install( TARGETS test_instructionset DESTINATION unittest )

################################################################################
#<KNOWHERE-COMMON-TEST>
add_executable( test_knowhere_common test_common.cpp
                                     utils.cpp )

target_link_libraries( test_knowhere_common
    PRIVATE     knowhere
                log
                ${TEST_LIBS} )

install( TARGETS test_knowhere_common DESTINATION unittest )

if (MILVUS_GPU_VERSION)
    ################################################################################
    #<GPU-RESOURCE-TEST>
    add_executable(test_gpuresource test_gpuresource.cpp utils.cpp)
    target_link_libraries(test_gpuresource PRIVATE knowhere log ${TEST_LIBS})
    install(TARGETS test_gpuresource DESTINATION unittest)

    ################################################################################
    #<CUSTOMIZED-INDEX-TEST>
    add_executable(test_customized_index test_customized_index.cpp utils.cpp)
    target_link_libraries(test_customized_index PRIVATE knowhere log ${TEST_LIBS})
    install(TARGETS test_customized_index DESTINATION unittest)
endif ()

################################################################################
#<IDMAP-TEST>
add_executable( test_idmap  test_idmap.cpp
                            utils.cpp )

target_link_libraries( test_idmap
    PRIVATE     knowhere
                log
                ${TEST_LIBS} )

install(TARGETS test_idmap DESTINATION unittest)

################################################################################
#<IVF-TEST>
add_executable( test_ivf test_ivf.cpp utils.cpp )
target_link_libraries( test_ivf
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )
    
install( TARGETS test_ivf DESTINATION unittest )

################################################################################
#<IVFNM-TEST-CPU>
add_executable( test_ivf_cpu_nm     test_ivf_cpu_nm.cpp
                                    utils.cpp )

target_link_libraries( test_ivf_cpu_nm
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_ivf_cpu_nm DESTINATION unittest )

################################################################################
#<IVFNM-TEST-GPU>
add_executable( test_ivf_gpu_nm     test_ivf_gpu_nm.cpp
                                    utils.cpp )

target_link_libraries(test_ivf_gpu_nm
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_ivf_gpu_nm DESTINATION unittest )

################################################################################
#<BinaryIDMAP-TEST>
add_executable( test_binaryidmap     test_binaryidmap.cpp
                                     utils.cpp )

target_link_libraries( test_binaryidmap
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_binaryidmap DESTINATION unittest )

################################################################################
#<BinaryIVF-TEST>
add_executable( test_binaryivf  test_binaryivf.cpp
                                utils.cpp )

target_link_libraries( test_binaryivf
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_binaryivf DESTINATION unittest )


################################################################################
#<NSG-TEST>
add_executable( test_nsg    test_nsg.cpp
                            utils.cpp )

target_link_libraries( test_nsg
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_nsg DESTINATION unittest )

################################################################################
#<HNSW-TEST>
add_executable( test_hnsw   test_hnsw.cpp
                            utils.cpp )

target_link_libraries( test_hnsw
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_hnsw DESTINATION unittest )

################################################################################
#<RHNSW_FLAT-TEST>
add_executable( test_rhnsw_flat     test_rhnsw_flat.cpp
                                    utils.cpp )
                                
target_link_libraries( test_rhnsw_flat
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_rhnsw_flat DESTINATION unittest )

################################################################################
#<RHNSW_PQ-TEST>
add_executable( test_rhnsw_pq   test_rhnsw_pq.cpp
                                utils.cpp )

target_link_libraries( test_rhnsw_pq
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_rhnsw_pq DESTINATION unittest )

################################################################################
#<RHNSW_SQ8-TEST>
add_executable( test_rhnsw_sq8  test_rhnsw_sq8.cpp
                                utils.cpp )

target_link_libraries( test_rhnsw_sq8
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_rhnsw_sq8 DESTINATION unittest )

################################################################################
#<SPTAG-TEST>
if (MILVUS_SUPPORT_SPTAG)
    add_executable( test_sptag test_sptag.cpp utils.cpp )
    target_link_libraries(test_sptag
        PRIVATE     SPTAGLibStatic
                    knowhere
                    log
                    ${TEST_LIBS}
                    )
    install(TARGETS test_sptag DESTINATION unittest)
endif ()

################################################################################
#<ANNOY-TEST>
add_executable( test_annoy      test_annoy.cpp
                                utils.cpp )

target_link_libraries( test_annoy
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_annoy DESTINATION unittest )

################################################################################
#<STRUCTURED-INDEX-SORT-TEST>
add_executable( test_structured_index_sort  test_structured_index_sort.cpp
                                            utils.cpp )

target_link_libraries( test_structured_index_sort
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_structured_index_sort DESTINATION unittest )

################################################################################
#<NGTPANNG-TEST>
add_executable( test_ngtpanng   test_ngtpanng.cpp
                                utils.cpp )

target_link_libraries( test_ngtpanng
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_ngtpanng DESTINATION unittest )

################################################################################
#<NGTPANNG-TEST>
add_executable( test_ngtonng test_ngtonng.cpp
                                utils.cpp )

target_link_libraries( test_ngtonng
    PRIVATE     knowhere
                log
                ${TEST_LIBS}
                )

install( TARGETS test_ngtonng DESTINATION unittest )

################################################################################
#add_subdirectory(faiss_benchmark)
#add_subdirectory(metric_alg_benchmark)
