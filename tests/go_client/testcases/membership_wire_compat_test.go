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

package testcases

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

type membershipWireKindFixture struct {
	Domain         string  `json:"domain"`
	Members        []int64 `json:"members"`
	PresentMembers []int64 `json:"present_members"`
	FPR            float64 `json:"fpr"`
	Hex            string  `json:"hex"`
}

type membershipWireFixture struct {
	SchemaVersion int                       `json:"schema_version"`
	Bloom         membershipWireKindFixture `json:"bloom"`
	Roaring       membershipWireKindFixture `json:"roaring"`
}

func loadMembershipWireFixture(t *testing.T) membershipWireFixture {
	t.Helper()
	path := membershipWireFixturePath(t)
	payload, err := os.ReadFile(path)
	require.NoError(t, err)

	var fixture membershipWireFixture
	require.NoError(t, json.Unmarshal(payload, &fixture))
	require.Equal(t, 1, fixture.SchemaVersion)
	require.Equal(t, "int64", fixture.Bloom.Domain)
	require.Equal(t, "int64", fixture.Roaring.Domain)
	return fixture
}

func membershipWireFixturePath(t *testing.T) string {
	t.Helper()
	const manifest = "manifest.json"
	candidates := make([]string, 0, 8)
	if _, sourceFile, _, ok := runtime.Caller(0); ok && filepath.IsAbs(sourceFile) {
		candidates = append(candidates,
			filepath.Join(filepath.Dir(sourceFile), "..", "..", "fixtures", "membership_filter", manifest))
	}

	workingDir, err := os.Getwd()
	require.NoError(t, err)
	for dir := workingDir; ; dir = filepath.Dir(dir) {
		candidates = append(candidates,
			filepath.Join(dir, "tests", "fixtures", "membership_filter", manifest),
			filepath.Join(dir, "fixtures", "membership_filter", manifest))
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	t.Fatalf("membership wire fixture not found; searched %v", candidates)
	return ""
}

func int64MemberSet(values []int64) map[int64]struct{} {
	result := make(map[int64]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func TestMembershipWireFixtureManifest(t *testing.T) {
	fixture := loadMembershipWireFixture(t)
	require.NotEmpty(t, fixture.Bloom.Hex)
	require.NotEmpty(t, fixture.Roaring.Hex)
}

func TestMembershipPythonGoldenBytes(t *testing.T) {
	fixture := loadMembershipWireFixture(t)
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	// Both golden blobs target the same INT64 membership schema and read-only
	// sealed fixture, so build/load it once and exercise both wire formats on the
	// same collection.
	collectionName := common.GenRandomString("membership_wire", 6)
	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	t.Run("bloom", func(t *testing.T) {
		pythonBlob, err := hex.DecodeString(fixture.Bloom.Hex)
		require.NoError(t, err)
		goBlob, err := client.NewBloomFilterBlob(fixture.Bloom.Members, fixture.Bloom.FPR)
		require.NoError(t, err)
		require.Equal(t, pythonBlob, []byte(goBlob), "Go and pymilvus bloom builders must share the wire format")

		got := queryMembershipIDs(t, ctx, mc, collectionName,
			fmt.Sprintf("membership_match(%s, {bf}, type=bloom)", membershipCreatorField), "bf", client.BloomFilterBlob(pythonBlob))
		expected := expectedMembershipIDs(membershipTotalRows, membershipDomain, int64MemberSet(fixture.Bloom.PresentMembers))
		requireBloomResult(t, int64IDSet(got), int64IDSet(expected), membershipTotalRows,
			"pymilvus-built golden Bloom blob")
	})

	t.Run("roaring", func(t *testing.T) {
		pythonBlob, err := hex.DecodeString(fixture.Roaring.Hex)
		require.NoError(t, err)
		goBlob, err := client.NewRoaringBitmapBlob(fixture.Roaring.Members)
		require.NoError(t, err)
		require.Equal(t, pythonBlob, []byte(goBlob), "Go and pymilvus roaring builders must share the wire format")

		got := queryMembershipIDs(t, ctx, mc, collectionName,
			fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), "rb", client.RoaringBitmapBlob(pythonBlob))
		expected := expectedMembershipIDs(membershipTotalRows, membershipDomain, int64MemberSet(fixture.Roaring.PresentMembers))
		require.ElementsMatch(t, expected, got, "pymilvus-built roaring blob result mismatch")
	})
}
