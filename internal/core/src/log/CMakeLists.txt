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
set(LOG_FILES   ${MILVUS_ENGINE_SRC}/log/Log.cpp
                ${MILVUS_ENGINE_SRC}/log/Log.h
                ${MILVUS_ENGINE_SRC}/log/LogMgr.cpp
                ${MILVUS_ENGINE_SRC}/log/LogMgr.h
                ${MILVUS_THIRDPARTY_SRC}/easyloggingpp/easylogging++.cc
                ${MILVUS_THIRDPARTY_SRC}/easyloggingpp/easylogging++.h
                )

add_library(log STATIC ${LOG_FILES})
set_target_properties(log PROPERTIES RULE_LAUNCH_COMPILE "")
set_target_properties(log PROPERTIES RULE_LAUNCH_LINK "")
target_link_libraries( log PRIVATE fiu )
