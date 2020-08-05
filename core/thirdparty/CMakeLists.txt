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

# ****************************** Thirdparty googletest ***************************************
add_subdirectory( gtest )
get_target_property( GTEST_INCLUDE_DIR gtest INTERFACE_INCLUDE_DIRECTORIES )

# ****************************** Thirdpart Common Config **************************************
# INCLUDE DIRS need to be set at parent coupe.
set( THIRDPARTY_INCLUDE_DIRS
        ${GTEST_INCLUDE_DIR} 
        PARENT_SCOPE )
