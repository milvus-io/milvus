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

enable_testing()

set( CONFIG_SRCS    
	 	${MILVUS_ENGINE_SRC}/config/ConfigMgr.cpp
	        ${MILVUS_ENGINE_SRC}/config/ConfigType.cpp
	        ${MILVUS_ENGINE_SRC}/config/ServerConfig.cpp
                    )

set(unittest_srcs
	ConfigTypeTest1.cpp
#	ConfigTypeTest2.cpp
        )


add_executable(test_config ${unittest_srcs} ${CONFIG_SRCS})
#add_executable(ServerConfigTest ServerConfigTest.cpp ${CONFIG_SRCS})
#add_executable(ConfigTypeTest1 ConfigTypeTest1.cpp ${CONFIG_SRCS})
#add_executable(ConfigTypeTest2 ConfigTypeTest2.cpp ${CONFIG_SRCS})

#target_link_libraries(ServerConfigTest ${UNITTEST_LIBS})
#target_link_libraries(ConfigTypeTest1 ${UNITTEST_LIBS})
#target_link_libraries(ConfigTypeTest2 ${UNITTEST_LIBS})
target_link_libraries(test_config ${UNITTEST_LIBS})

install(TARGETS test_config DESTINATION unittest)

#install(TARGETS ServerConfigTest DESTINATION unittest)
#install(TARGETS ConfigTypeTest1 DESTINATION unittest)
#install(TARGETS ConfigTypeTest2 DESTINATION unittest)
