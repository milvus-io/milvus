# ARROW
if (DEFINED ENV{KNOWHERE_ARROW_URL})
    set(ARROW_SOURCE_URL "$ENV{KNOWHERE_ARROW_URL}")
else ()
    set(ARROW_SOURCE_URL
            "https://github.com/apache/arrow.git"
            )
endif ()

set(ARROW_PREFIX "${INDEX_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep/cpp")

macro(build_arrow)
    message(STATUS "Building Apache ARROW-${ARROW_VERSION} from source")
    set(ARROW_STATIC_LIB_NAME arrow)
    set(ARROW_LIB_DIR "${ARROW_PREFIX}/lib")
    set(ARROW_STATIC_LIB
            "${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ARROW_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )
    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")

    set(ARROW_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DARROW_BUILD_STATIC=ON
            -DARROW_BUILD_SHARED=OFF
            -DARROW_USE_GLOG=OFF
            -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
            -DCMAKE_INSTALL_LIBDIR=${ARROW_LIB_DIR}
            -DARROW_CUDA=OFF
            -DARROW_FLIGHT=OFF
            -DARROW_GANDIVA=OFF
            -DARROW_GANDIVA_JAVA=OFF
            -DARROW_HDFS=OFF
            -DARROW_HIVESERVER2=OFF
            -DARROW_ORC=OFF
            -DARROW_PARQUET=OFF
            -DARROW_PLASMA=OFF
            -DARROW_PLASMA_JAVA_CLIENT=OFF
            -DARROW_PYTHON=OFF
            -DARROW_WITH_BZ2=OFF
            -DARROW_WITH_ZLIB=OFF
            -DARROW_WITH_LZ4=OFF
            -DARROW_WITH_SNAPPY=OFF
            -DARROW_WITH_ZSTD=OFF
            -DARROW_WITH_BROTLI=OFF
            -DCMAKE_BUILD_TYPE=Release
            -DARROW_DEPENDENCY_SOURCE=BUNDLED #Build all arrow dependencies from source instead of calling find_package first
            -DBOOST_SOURCE=AUTO #try to find BOOST in the system default locations and build from source if not found
            )

    externalproject_add(arrow_ep
            GIT_REPOSITORY
            ${ARROW_SOURCE_URL}
            GIT_TAG
            ${ARROW_VERSION}
            GIT_SHALLOW
            TRUE
            SOURCE_SUBDIR
            cpp
            ${EP_LOG_OPTIONS}
            CMAKE_ARGS
            ${ARROW_CMAKE_ARGS}
            BUILD_COMMAND
            ""
            INSTALL_COMMAND
            ${MAKE} ${MAKE_BUILD_ARGS} install
            BUILD_BYPRODUCTS
            "${ARROW_STATIC_LIB}"
            )

    file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
    #在指定目录下创建子目录
    add_library(arrow STATIC IMPORTED)
    #这里指定CMake args之后就可以直接操作了
    set_target_properties(arrow
            PROPERTIES IMPORTED_LOCATION "${ARROW_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}")
    add_dependencies(arrow arrow_ep)

    set(JEMALLOC_PREFIX "${INDEX_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep-build/jemalloc_ep-prefix/src/jemalloc_ep")

    add_custom_command(TARGET arrow_ep POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory ${ARROW_LIB_DIR}
            COMMAND ${CMAKE_COMMAND} -E copy ${JEMALLOC_PREFIX}/lib/libjemalloc_pic.a ${ARROW_LIB_DIR}
            DEPENDS ${JEMALLOC_PREFIX}/lib/libjemalloc_pic.a)

endmacro()

if (KNOWHERE_WITH_ARROW AND NOT TARGET arrow_ep)

    if (Arrow_SOURCE STREQUAL "AUTO")
        find_package(Arrow MODULE)
        if (NOT Arrow_FOUND)
            build_arrow()
        endif ()
    elseif (Arrow_SOURCE STREQUAL "BUNDLED")
        build_arrow()
    elseif (Arrow_SOURCE STREQUAL "SYSTEM")
        find_package(Arrow REQUIRED)
    endif ()

    link_directories(SYSTEM ${ARROW_LIB_DIR})
    include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
    #"Method to use for acquiring KNOWHERE's build dependencies"
    #以后就可以通过SYSTEM找到这个库了？
    #虽然这里定义了arrow 但显然arrow并不是一个每次都运行的库
    #为了更通用的完成一些工作，
endif ()