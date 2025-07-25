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

FetchContent_Declare(
    unordered_dense
    URL https://github.com/martinus/unordered_dense/archive/refs/tags/v4.5.0.tar.gz
    URL_HASH MD5=a8426ee52ff15b77f3f9d9de1f10f171
)
FetchContent_MakeAvailable(unordered_dense)

set( UNORDERED_DENSE_INCLUDE_DIR ${unordered_dense_SOURCE_DIR}/include CACHE INTERNAL "Path to unordered_dense include directory" ) 