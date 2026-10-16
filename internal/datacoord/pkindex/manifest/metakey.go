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

const (
	// PointerPrefix is the meta key prefix of all manifest pointers.
	PointerPrefix = "pkindex-meta/manifest/"
	// ClaimPrefix is the meta key prefix of all pchannel claim records.
	ClaimPrefix = "pkindex-meta/claim/"
)

// PointerKey returns the meta key of the manifest pointer of a vchannel.
func PointerKey(collectionID int64, vchannel string) string {
	return PointerPrefix + strconv.FormatInt(collectionID, 10) + "/" + vchannel
}

// ClaimKey returns the meta key of the claim record of a pchannel.
func ClaimKey(pchannel string) string {
	return ClaimPrefix + pchannel
}

// ParsePointerKey returns the collection ID and the vchannel of a pointer key.
func ParsePointerKey(key string) (int64, string, bool) {
	rest, ok := strings.CutPrefix(key, PointerPrefix)
	if !ok {
		return 0, "", false
	}
	coll, vchannel, ok := strings.Cut(rest, "/")
	if !ok || vchannel == "" || strings.Contains(vchannel, "/") {
		return 0, "", false
	}
	id, err := strconv.ParseInt(coll, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return id, vchannel, true
}
