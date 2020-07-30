
set( GRPC_SERVICE_FILES ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
                        )

aux_source_directory( ${MILVUS_ENGINE_SRC}/server                   SERVER_SERVICE_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/init              SERVER_INIT_FILES )
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

add_library( server STATIC)
target_sources( server
                PRIVATE ${GRPC_SERVER_FILES}
                        ${GRPC_SERVICE_FILES}
                        ${SERVER_FILES}
                        ${WEB_SERVER_FILES}
                        ${SERVER_CONTEXT_FILES}
                        )
