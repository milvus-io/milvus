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

package manifest

import (
	"strconv"
	"strings"
)

// The functions below build and parse the meta keys of the primary key index.
// The keys are relative to the root path of the DataCoord meta kv:
//
//	pkindex-meta/manifest/<collectionID>/<indexID>/<vchannel>    ManifestPointer{version, body_key}
//
// For example: pkindex-meta/manifest/449/451/by-dev-rootcoord-dml_0_449v0
//
// A flush also reads streamingcoord-meta/pchannel/<pchannel> (PChannelMeta)
// to check its term, and does not write it (see pchannel_term.go).

// PointerPrefix is the meta key prefix of all manifest pointers.
const PointerPrefix = "pkindex-meta/manifest/"

// PointerKey returns the meta key of the manifest pointer of the index of a vchannel.
func PointerKey(collectionID, indexID int64, vchannel string) string {
	return PointerPrefix + strconv.FormatInt(collectionID, 10) + "/" + strconv.FormatInt(indexID, 10) + "/" + vchannel
}

// ParsePointerKey returns the collection ID, the index ID and the vchannel of
// a pointer key.
func ParsePointerKey(key string) (collectionID, indexID int64, vchannel string, ok bool) {
	rest, ok := strings.CutPrefix(key, PointerPrefix)
	if !ok {
		return 0, 0, "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[2] == "" {
		return 0, 0, "", false
	}
	collectionID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	indexID, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	return collectionID, indexID, parts[2], true
}
