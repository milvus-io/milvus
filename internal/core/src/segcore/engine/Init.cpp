// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/engine/Init.h"
#include "common/Consts.h"
#include "segcore/engine/SegmentConnector.h"

namespace milvus::engine {
void
InitEngine() {
    facebook::velox::functions::prestosql::registerAllScalarFunctions();
    facebook::velox::parse::registerTypeResolver();
    // facebook::velox::functions::prestosql::registerAllScalarFunctions();
    auto segment_connector = facebook::velox::connector::getConnectorFactory(
                                 SegmentConnectorFactory::kSegmentConnectorName)
                                 ->newConnector(SEGMENT_CONNECTOR_ID, {});
    facebook::velox::connector::registerConnector(segment_connector);
}

}  // namespace milvus::engine
