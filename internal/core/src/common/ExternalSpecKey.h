// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include <string_view>

namespace milvus {

// FoldExternalSpecKey folds an external_spec key the way Go's
// strings.EqualFold, and so encoding/json, compares it with a lowercase ASCII
// key: ASCII letters fold to lowercase, and U+017F (long s) and U+212A (Kelvin
// sign) are the only other characters that fold to an ASCII letter. A key
// folds to a lowercase ASCII key exactly when Go reads it as that key, so Go
// reads a lone "Format" or "EXTFS" as format or extfs.
inline std::string
FoldExternalSpecKey(std::string_view key) {
    std::string folded;
    folded.reserve(key.size());
    for (size_t i = 0; i < key.size(); ++i) {
        if (key.substr(i, 2) == "\xC5\xBF") {
            folded.push_back('s');
            i += 1;
        } else if (key.substr(i, 3) == "\xE2\x84\xAA") {
            folded.push_back('k');
            i += 2;
        } else if (key[i] >= 'A' && key[i] <= 'Z') {
            folded.push_back(static_cast<char>(key[i] - 'A' + 'a'));
        } else {
            folded.push_back(key[i]);
        }
    }
    return folded;
}

}  // namespace milvus
