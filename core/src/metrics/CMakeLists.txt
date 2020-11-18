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

set( METRICS_SRCS   Prometheus.cpp
                    Prometheus.h
                    )

set( METRICS_LIBS   prometheus-cpp::core
                    prometheus-cpp::pull
                    prometheus-cpp::push
                    )

create_library(
        TARGET      metrics
        SRCS        ${METRICS_SRCS}
        LIBS        ${METRICS_LIBS}
)

