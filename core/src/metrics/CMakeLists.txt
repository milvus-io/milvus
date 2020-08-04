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

aux_source_directory( ${MILVUS_ENGINE_SRC}/metrics              METRICS_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/metrics/prometheus   METRICS_PROMETHEUS_FILES )

set( METRICS_LIB    yaml-cpp )

set( PROMETHEUS_LIB prometheus-cpp-push
                    prometheus-cpp-pull
                    prometheus-cpp-core
                    )

add_library(    metrics STATIC ${METRICS_FILES})

if ( MILVUS_WITH_PROMETHEUS )
    target_sources( metrics PRIVATE ${METRICS_PROMETHEUS_FILES} )
    list( APPEND metrics_lib ${PROMETHEUS_LIB} )
endif ()


target_link_libraries( metrics PRIVATE ${METRICS_LIB} )
add_dependencies( metrics ${PROMETHEUS_LIB} )
