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
aux_source_directory( ${MILVUS_ENGINE_SRC}/storage          STORAGE_MAIN_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/storage/disk     STORAGE_DISK_FILES )
# aux_source_directory( ${MILVUS_ENGINE_SRC}/storage/s3     STORAGE_S3_FILES )
set( STORAGE_FILES  ${STORAGE_MAIN_FILES}
                    ${STORAGE_DISK_FILES}
                    # ${STORAGE_S3_FILES}
                    )
add_library( storage STATIC )
target_sources( storage PRIVATE ${STORAGE_FILES} )

set ( LINK_LIBRARY fiu log )

if ( MILVUS_WITH_AWS )
    list( APPEND LINK_LIBRARY
                AWS::aws-cpp-sdk-s3
                AWS::aws-cpp-sdk-core )
endif()

target_link_libraries( storage PRIVATE ${LINK_LIBRARY} )
