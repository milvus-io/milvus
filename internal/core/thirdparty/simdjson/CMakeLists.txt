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
    URL https://github.com/simdjson/simdjson/archive/refs/tags/v5.0.1.tar.gz
    URL_HASH SHA256=8f7fcd06418b461d8b4d610004cb5e16b0887f7eef905021fda51a9c28277769
)
FetchContent_MakeAvailable(simdjson)

set( SIMDJSON_INCLUDE_DIR ${simdjson_SOURCE_DIR}/include CACHE INTERNAL "Path to simdjson include directory" )
