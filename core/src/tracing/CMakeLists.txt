aux_source_directory(${MILVUS_ENGINE_SRC}/tracing tracing_files)

add_library(tracing STATIC ${tracing_files} ${thirdparty_files})

set(tracing_lib
        opentracing
        opentracing_mocktracer
        pthread
        z
        dl
        )
target_link_libraries(tracing ${tracing_lib})
