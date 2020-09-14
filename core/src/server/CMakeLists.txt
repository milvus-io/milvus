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

set( GRPC_SERVICE_FILES ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
                        )

set( SERVER_INIT_FILES  ${MILVUS_ENGINE_SRC}/server/init/CpuChecker.cpp
                        ${MILVUS_ENGINE_SRC}/server/init/Directory.cpp
                        ${MILVUS_ENGINE_SRC}/server/init/GpuChecker.cpp
                        ${MILVUS_ENGINE_SRC}/server/init/Timezone.cpp
        )

aux_source_directory( ${MILVUS_ENGINE_SRC}/server                   SERVER_SERVICE_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/delivery/request  DELIVERY_REQUEST_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/delivery/strategy DELIVERY_STRATEGY_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/delivery          DELIVERY_FILES )

set( SERVER_FILES     ${SERVER_INIT_FILES}
                      ${SERVER_SERVICE_FILES}
                      ${SERVER_INIT_FILES}
                      ${DELIVERY_REQUEST_FILES}
                      ${DELIVERY_STRATEGY_FILES}
                      ${DELIVERY_FILES}
                      )

aux_source_directory( ${MILVUS_ENGINE_SRC}/server/grpc_impl             GRPC_IMPL_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/grpc_impl/interceptor GRPC_INTERCEPTOR_FILES )

set( GRPC_SERVER_FILES ${GRPC_IMPL_FILES}
                       ${GRPC_INTERCEPTOR_FILES}
                       )

aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/handler      WEB_HANDLER_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/component    WEB_CONPONENT_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/controller   WEB_CONTROLLER_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/dto          WEB_DTO_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/utils        WEB_UTILS_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl              WEB_IMPL_FILES )

set( WEB_SERVER_FILES ${WEB_HANDLER_FILES}
                      ${WEB_CONPONENT_FILES}
                      ${WEB_CONTROLLER_FILES}
                      ${WEB_DTO_FILES}
                      ${WEB_UTILS_FILES}
                      ${WEB_IMPL_FILES}
                      )

aux_source_directory( ${MILVUS_ENGINE_SRC}/server/context               SERVER_CONTEXT_FILES )

add_library( server STATIC )
target_sources( server
                PRIVATE ${GRPC_SERVER_FILES}
                        ${GRPC_SERVICE_FILES}
                        ${SERVER_FILES}
                        ${WEB_SERVER_FILES}
                        ${SERVER_CONTEXT_FILES}
                        )

add_subdirectory(init)
