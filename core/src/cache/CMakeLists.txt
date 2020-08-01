aux_source_directory( ${MILVUS_ENGINE_SRC}/cache CACHE_FILES )
add_library( cache STATIC )
target_sources( cache PRIVATE   ${CACHE_FILES}
                                CacheMgr.inl
                                Cache.inl
                                )
target_include_directories( cache PUBLIC ${MILVUS_ENGINE_SRC}/cache )
