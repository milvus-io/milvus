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

aux_source_directory( ${MILVUS_ENGINE_SRC}/segment          SEGMENT_FILES )
# Third Party dablooms file
aux_source_directory( ${MILVUS_THIRDPARTY_SRC}/dablooms     THIRDPARTY_DABLOOMS_FILES )
add_library( segment STATIC )
target_sources( segment PRIVATE ${SEGMENT_FILES}
                                ${THIRDPARTY_DABLOOMS_FILES}
                                )
add_dependencies( segment sqlite mysqlpp )
