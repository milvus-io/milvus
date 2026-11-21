// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "common/Types.h"
#include "segcore/UpdatesRecord.h"

namespace milvus::segcore {

enum class PkUpdateReadPath {
    RawPredicate,
    IndexedPredicate,
    Reverse,
    Overlay
};

struct PkUpdateReadEvent {
    PkUpdateReadPath path;
    size_t rows = 0;
    UpdatesRecord::ReadStats updates{};
    size_t visited_bitmap_bits = 0;
};

// Opt-in only on the benchmark's synchronous caller thread. A null callback
// leaves reads unobserved. The benchmark owns and scopes the callback lifetime.
using PkUpdateReadObserver = void (*)(FieldId, const PkUpdateReadEvent&);
extern thread_local PkUpdateReadObserver pk_update_read_observer;

inline void
ObservePkUpdateRead(FieldId field, const PkUpdateReadEvent& event) {
    if (auto observer = pk_update_read_observer) {
        observer(field, event);
    }
}

}  // namespace milvus::segcore
