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
    simdjson
    URL https://github.com/simdjson/simdjson/archive/refs/tags/v3.1.7.tar.gz
    URL_HASH MD5=1b0d75ad32179c77f84f4a09d4214057
)
FetchContent_MakeAvailable(simdjson)

set( SIMDJSON_INCLUDE_DIR ${simdjson_SOURCE_DIR}/include CACHE INTERNAL "Path to simdjson include directory" )
