#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------
set( STORAGE_SRCS
        ExtraFileInfo.cpp
        ExtraFileInfo.h
        FSHandler.h
        IOReader.h
        IOWriter.h
        Operation.h
        disk/DiskIOReader.h
        disk/DiskIOWriter.h
        disk/DiskOperation.h
        disk/DiskIOReader.cpp
        disk/DiskIOWriter.cpp
        disk/DiskOperation.cpp
#        s3/S3ClientMock.h
#        s3/S3ClientWrapper.h
#        s3/S3ClientWrapper.cpp
#        s3/S3IOReader.h
#        s3/S3IOReader.cpp
#        s3/S3IOWriter.h
#        s3/S3IOWriter.cpp
        )
#aux_source_directory( ${MILVUS_ENGINE_SRC}/storage          STORAGE_MAIN_FILES )
#aux_source_directory( ${MILVUS_ENGINE_SRC}/storage/disk     STORAGE_DISK_FILES )
# aux_source_directory( ${MILVUS_ENGINE_SRC}/storage/s3     STORAGE_S3_FILES )
#set( STORAGE_FILES  ${STORAGE_MAIN_FILES}
#                    ${STORAGE_DISK_FILES}
                    # ${STORAGE_S3_FILES}
#                    )
add_library( storage STATIC )
target_sources( storage PRIVATE ${STORAGE_SRCS} )
add_dependencies( storage fiu )

target_link_libraries( storage
        log
        crc32c
        fiu
        libboost_filesystem.a
        libstdc++fs.a
        )


if ( BUILD_UNIT_TEST )
    add_executable( ExtraFileInfoTest )
    target_sources( ExtraFileInfoTest PRIVATE ExtraFileInfoTest.cpp)
    target_link_libraries( ExtraFileInfoTest
            storage
            gtest
            gtest_main
            gmock
            gmock_main )

    add_test ( NAME     ExtraFileInfoTest
            COMMAND  $<TARGET_FILE:ExtraFileInfoTest>
            )
endif()
