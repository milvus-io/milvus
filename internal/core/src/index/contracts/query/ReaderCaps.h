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

// Capabilities of one index, derived from metadata and cached before payload
// open. Consumers choose a path without pinning cold indexes just to discover
// their interfaces. The opened reader's Caps() must match that cached copy.
// Capabilities describe one entry; combining bits from unrelated indexes can
// describe an interface set no individual reader actually provides.

namespace milvus::index {

struct ReaderCaps {
    // In / NotIn / Range. INullReader is a separate cross-family interface and
    // has no capability bit here.
    bool predicate = false;

    // LIKE family: Match / PrefixMatch / PostfixMatch / InnerMatch.
    bool pattern_match = false;

    bool text_match = false;

    // Candidate family: the hit set is a SUPERSET, exec must verify.
    bool ngram_candidates = false;

    // Candidate family: spatial relation predicates (MBR coarse filter).
    bool spatial = false;

    // Hits are element offsets. The consumer decides where to fold them to rows;
    // the index does not own or apply the column's offsets.
    bool nested = false;

    // Can look the original value back up.
    bool value_lookup = false;

    // Per-offset reverse lookup has low cost, typically O(1) or O(log n).
    // The consumer decides whether to use it or read the raw column.
    bool cheap_value_lookup = false;

    // Composite path-addressed index, not a shredded JSON column layout.
    bool json_paths = false;

    // false => the hit set is a superset (ngram, spatial, nested ARRAY equality).
    bool exact = true;
};

}  // namespace milvus::index
