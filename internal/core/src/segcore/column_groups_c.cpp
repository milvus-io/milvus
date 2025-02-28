// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/column_groups_c.h"
#include <vector>
#include <string>
#include <memory>

using VecVecInt = std::vector<std::vector<int>>;

extern "C" {

CColumnGroups
NewCColumnGroups() {
    auto vv = std::make_unique<VecVecInt>();
    return vv.release();
}

void
AddCColumnGroup(CColumnGroups cgs, int* group, int group_size) {
    if (!cgs || !group)
        return;

    auto vv = static_cast<VecVecInt*>(cgs);
    std::vector<int> new_group(group, group + group_size);
    vv->emplace_back(std::move(new_group));
}

int
CColumnGroupsSize(CColumnGroups cgs) {
    if (!cgs)
        return 0;

    auto vv = static_cast<VecVecInt*>(cgs);
    return static_cast<int>(vv->size());
}

void
FreeCColumnGroups(CColumnGroups cgs) {
    delete static_cast<VecVecInt*>(cgs);
}
}