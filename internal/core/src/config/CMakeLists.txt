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

# library
set( CONFIG_SRCS    ConfigMgr.h
                    ConfigMgr.cpp
                    ConfigType.h
                    ConfigType.cpp
                    ServerConfig.h
                    ServerConfig.cpp
                    )

set( CONFIG_LIBS    yaml-cpp
                    )

create_library(
        TARGET      config
        SRCS        ${CONFIG_SRCS}
        LIBS        ${CONFIG_LIBS}
)

