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

//go:build test

package pkindex

import (
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

// FakeManifest is a manifest of one vchannel as a fake manifest service
// keeps it. Tests use it to fake a Committer.
// FakeClaimPChannel applies a claim of a pchannel for term to the claimed
// term cur (0 when the pchannel was never claimed), with the rules of the
// manifest service. It returns the claimed term after the claim:
//
//   - a lower term: index.ErrFenced,
//   - the same term: cur,
//   - a higher term or no claim: term.
func FakeClaimPChannel(pchannel string, cur, term int64) (int64, error) {
	if term < cur {
		return cur, errors.Wrapf(index.ErrFenced, "pchannel %s: claimed term %d, claim term %d", pchannel, cur, term)
	}
	return term, nil
}

// FakeRecoveryManifest returns the answer for the manifest cur (nil when
// the vchannel has none). The body key names the vchannel and the version.
func FakeRecoveryManifest(vchannel string, cur *index.FakeManifest) RecoveryManifest {
	if cur == nil {
		return RecoveryManifest{}
	}
	return RecoveryManifest{Found: true, Version: cur.View.Version, BodyKey: "manifest/" + vchannel + "/" + strconv.FormatInt(cur.View.Version, 10) + ".pb"}
}
