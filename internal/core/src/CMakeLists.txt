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

include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})


set(FOUND_OPENBLAS "unknown")
add_subdirectory(index)
set(INDEX_INCLUDE_DIRS ${INDEX_INCLUDE_DIRS} PARENT_SCOPE)
foreach (DIR ${INDEX_INCLUDE_DIRS})
    include_directories(${DIR})
endforeach ()


add_subdirectory( exceptions )
add_subdirectory( utils )
add_subdirectory( log )
add_subdirectory( pb )
add_subdirectory( storage )
add_subdirectory( segcore )
add_subdirectory( cache )
add_subdirectory( query )
add_subdirectory( common )
add_subdirectory( indexbuilder )
add_subdirectory( config )
