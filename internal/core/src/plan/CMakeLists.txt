# Copyright (C) 2019-2025 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

add_source_at_current_directory_recursively()
add_library(milvus_plan OBJECT ${SOURCE_FILES})
# this is to ensure the plan parser C header can be found by the compiler.
target_include_directories(milvus_plan PRIVATE ${MILVUS_SOURCE_DIR}/output/include)

find_program(GO_EXECUTABLE go)
if (NOT GO_EXECUTABLE)
    message(FATAL_ERROR "Go executable not found!")
endif()

set(PLAN_PARSER_OUTPUT_LIB "${MILVUS_SOURCE_DIR}/output/lib/libmilvus-planparser.a")
set(PLAN_PARSER_OUTPUT_HEADER "${MILVUS_SOURCE_DIR}/output/include/libmilvus-planparser.h")
set(PLAN_PARSER_GO_SOURCE "${MILVUS_SOURCE_DIR}/../parser/planparserv2/cwrapper/wrapper.go")
set(PLAN_PARSER_CPP_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/milvus_plan_parser.h")
set(PLAN_PARSER_OUTPUT_CPP_HEADER "${MILVUS_SOURCE_DIR}/output/include/milvus_plan_parser.h")

add_custom_command(
    OUTPUT ${PLAN_PARSER_OUTPUT_LIB} ${PLAN_PARSER_OUTPUT_HEADER} ${PLAN_PARSER_OUTPUT_CPP_HEADER}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${MILVUS_SOURCE_DIR}/output/lib
    COMMAND ${CMAKE_COMMAND} -E make_directory ${MILVUS_SOURCE_DIR}/output/include
    COMMAND ${CMAKE_COMMAND} -E env CGO_ENABLED=1 GO111MODULE=on ${GO_EXECUTABLE} build -buildmode=c-archive -o ${PLAN_PARSER_OUTPUT_LIB} ${PLAN_PARSER_GO_SOURCE}
    COMMAND ${CMAKE_COMMAND} -E rename ${MILVUS_SOURCE_DIR}/output/lib/libmilvus-planparser.h ${PLAN_PARSER_OUTPUT_HEADER}
    COMMAND ${CMAKE_COMMAND} -E copy ${PLAN_PARSER_CPP_HEADER} ${PLAN_PARSER_OUTPUT_CPP_HEADER}
    DEPENDS ${PLAN_PARSER_GO_SOURCE} ${PLAN_PARSER_CPP_HEADER}
    COMMENT "Building plan parser archive library..."
)

add_custom_target(plan_parser_go ALL DEPENDS ${PLAN_PARSER_OUTPUT_LIB})
add_dependencies(milvus_plan plan_parser_go)
