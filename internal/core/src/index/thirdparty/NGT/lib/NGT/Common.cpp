//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "NGT/lib/NGT/Common.h"
#include "NGT/lib/NGT/ObjectSpace.h"

int64_t
NGT::SearchContainer::memSize() {
    auto workres_size = workingResult.size() == 0 ? 0 : workingResult.size() * workingResult.top().memSize();
    return sizeof(size_t) * 3 + sizeof(float) * 3 + result->memSize() + 1 + workres_size + Container::memSize();
}
