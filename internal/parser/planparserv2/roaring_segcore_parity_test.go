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

package planparserv2

import (
	"testing"

	serverroaring "github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
)

var segcoreRoaringSources = []string{
	"internal/core/src/common/RoaringMembership.h",
	"internal/core/src/common/RoaringMembership.cpp",
}

// The MRB1 envelope and admission constants are one contract across three
// implementations: the SDK builder (client/v3/roaringfilter), the proxy
// validator (pkg/v3/util/roaringfilter) and segcore
// (internal/core/src/common/RoaringMembership.*). The SDK pre-rejects what the
// proxy would reject so a caller fails locally instead of after a round trip,
// and the proxy pre-rejects what segcore would reject so a hostile blob is not
// fanned out.
//
// The two Go copies are pinned to each other by
// TestClientBuiltBlobsPassProxyValidation, in this package because it is the
// only one importing both modules. These are the C++ copy. segcore does consume
// them -- RoaringMembership.cpp reads all of them -- but nothing pinned them to
// the Go values, so editing kMaxHighContainerCount there alone compiled and
// passed CI.
//
// Pinned against the proxy validator's copy rather than the SDK's because the
// proxy is the side segcore has to agree with: a blob only reaches a querynode
// after Validate admitted it. The SDK's copy is held to the same values by
// TestClientBuiltBlobsPassProxyValidation.
var segcoreRoaringPins = []cppConstantPin{
	pinCppString("kMagic", serverroaring.Magic),
	pinCppInt("uint16_t", "kVersion", uint64(serverroaring.Version)),
	pinCppInt("uint16_t", "kFormatPortableRoaring64", uint64(serverroaring.FormatPortableRoaring64)),
	pinCppInt("size_t", "kHeaderSize", serverroaring.HeaderSize),
	pinCppInt("size_t", "kMaxBodySize", serverroaring.MaxBodyBytes),
	pinCppInt("uint64_t", "kMaxHighContainerCount", serverroaring.MaxHighContainerCount),
	pinCppInt("uint64_t", "kMaxEstimatedDecodedBytes", serverroaring.MaxEstimatedDecodedBytes),
	pinCppInt("uint64_t", "kEstimatedHighContainerOverheadBytes", serverroaring.EstimatedHighContainerOverheadBytes),
	pinCppInt("uint64_t", "kEstimatedLowContainerOverheadBytes", serverroaring.EstimatedLowContainerOverheadBytes),
}

// The portable-Roaring wire constants are deliberately not pinned by value.
// They are RoaringFormatSpec values rather than a Milvus contract -- both sides
// implement a published format, and two of them are derived rather than chosen
// (kPortableBitmapBytes is 65536/8, kPortableRoaring64MinEntryBytes is the
// 4-byte high key plus the 8-byte minimum child).
//
// Each side is instead held to the format behaviourally, by tests written
// independently that exercise the value at its boundary rather than reading it:
//
//   - the container-type threshold and the bitmap payload size: the Go
//     validator accepts a CRoaring-written bitmap-container fixture
//     (TestValidateAcceptsCRoaringGeneratedFixtures) and segcore accepts the
//     Go-written one (ParsesGoGeneratedBitmapFixture). Moving the threshold
//     makes either side read a 5000-entry array where the bytes hold an
//     8192-byte bitmap, so the length stops adding up;
//   - the run-cookie offset threshold: AcceptsValidPortableContainerEncodings
//     asserts both sides of it, three containers with no offset table and four
//     with one;
//   - the cookies and the 8-byte Roaring64 prefix appear in every fixture;
//   - the minimum-entry bound:
//     RejectsUnsupportedRoaring32CookieAfterPrefixBound pads to exactly that
//     minimum and asserts the failure is the cookie rather than the bound, so
//     raising it fails.
//
// Those checks catch a divergence a value pin cannot: a value pin reads
// declarations, so flipping a `>` to a `>=` passes it while the fixtures fail.
// They are not strictly stronger, and the limit is worth stating because it was
// measured rather than assumed. Fault injection on
// kPortableRoaring64MinEntryBytes, the weakest of the seven:
//
//   - tightening it (12 -> 16) fails on both sides --
//     TestValidateAcceptsRunCookieWithSingleValueArrayContainer pins a 23-byte
//     body in Go, RejectsUnsupportedRoaring32CookieAfterPrefixBound pads to
//     exactly the bound in C++;
//   - loosening it (12 -> 8) fails on neither.
//
// Loosening is benign here -- the bound is a cheap pre-filter, a structurally
// valid body always carries at least 12 bytes per high entry, and a body that
// slips past a loosened bound is rejected by the full scan behind it -- but the
// two sides really can drift in that direction without a test noticing. What
// this list gives up is symmetry: these tests hold each implementation to the
// format, not to each other. They stay listed so a constant added to the .cpp
// still has to be classified.
var segcoreRoaringUnpinned = []string{
	"kPortableCookieNoRun",
	"kPortableCookieRun",
	"kPortableArrayMaxCardinality",
	"kPortableBitmapBytes",
	"kPortableNoOffsetThreshold",
	"kPortableRoaring64PrefixBytes",
	"kPortableRoaring64MinEntryBytes",
}

// TestRoaringSegcoreConstantsMatch fails when an MRB1 constant is changed on
// one side of the cgo boundary only. See cpp_constant_parity_test.go for how
// the comparison works and what it does not cover.
func TestRoaringSegcoreConstantsMatch(t *testing.T) {
	assertCppConstantParity(t, segcoreRoaringSources, segcoreRoaringPins, segcoreRoaringUnpinned)
}
