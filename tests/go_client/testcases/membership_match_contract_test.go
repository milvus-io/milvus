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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestMembershipMatchUnifiedContract pins the public contract introduced by
// #52778: magic-based dispatch is equivalent to an explicit type pin, a pin
// must agree with the blob magic, unknown magic fails closed, and the two
// unreleased predecessor function names remain rejected.
func TestMembershipMatchUnifiedContract(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("membership_contract", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	bloomBlob, err := client.NewBloomFilterBlob([]int64{0, 1, 2, 42}, 0.001)
	require.NoError(t, err)
	roaringBlob, err := client.NewRoaringBitmapBlob([]int64{0, 1, 2, 42})
	require.NoError(t, err)

	t.Run("auto dispatch matches explicit type", func(t *testing.T) {
		bloomAuto := queryMembershipIDs(t, ctx, mc, collectionName,
			fmt.Sprintf("membership_match(%s, {bf})", membershipCreatorField), "bf", bloomBlob)
		bloomPinned := queryMembershipIDs(t, ctx, mc, collectionName,
			fmt.Sprintf("membership_match(%s, {bf}, type=bloom)", membershipCreatorField), "bf", bloomBlob)
		expectedBloom := expectedMembershipIDs(membershipTotalRows, membershipDomain,
			map[int64]struct{}{0: {}, 1: {}, 2: {}, 42: {}})
		requireBloomResult(t, int64IDSet(bloomAuto), int64IDSet(expectedBloom), membershipTotalRows,
			"auto-dispatched Bloom")
		requireBloomResult(t, int64IDSet(bloomPinned), int64IDSet(expectedBloom), membershipTotalRows,
			"explicit Bloom")
		require.ElementsMatch(t, bloomPinned, bloomAuto)

		roaringAuto := queryMembershipIDs(t, ctx, mc, collectionName,
			fmt.Sprintf("membership_match(%s, {rb})", membershipCreatorField), "rb", roaringBlob)
		roaringPinned := queryMembershipIDs(t, ctx, mc, collectionName,
			fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), "rb", roaringBlob)
		require.ElementsMatch(t, roaringPinned, roaringAuto)
	})

	queryErr := func(expr, key string, blob any) error {
		opt := client.NewQueryOption(collectionName).
			WithFilter(expr).
			WithOutputFields("id").
			WithConsistencyLevel(entity.ClStrong).
			WithTemplateParam(key, blob)
		_, err := mc.Query(ctx, opt)
		return err
	}

	t.Run("explicit type must match magic", func(t *testing.T) {
		err := queryErr(fmt.Sprintf("membership_match(%s, {bf}, type=roaring)", membershipCreatorField), "bf", bloomBlob)
		require.ErrorContains(t, err, "does not match filter blob format")

		err = queryErr(fmt.Sprintf("membership_match(%s, {rb}, type=bloom)", membershipCreatorField), "rb", roaringBlob)
		require.ErrorContains(t, err, "does not match filter blob format")
	})

	t.Run("unknown magic fails closed", func(t *testing.T) {
		err := queryErr(fmt.Sprintf("membership_match(%s, {blob})", membershipCreatorField), "blob",
			client.BloomFilterBlob([]byte("UNKNOWN-MEMBERSHIP-FORMAT")))
		require.ErrorContains(t, err, "unknown format magic")
	})

	t.Run("predecessor names are rejected", func(t *testing.T) {
		cases := []struct {
			expr string
			key  string
			blob any
		}{
			{fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField), "bf", bloomBlob},
			{fmt.Sprintf("roaring_match(%s, {rb})", membershipCreatorField), "rb", roaringBlob},
		}
		for _, tc := range cases {
			err := queryErr(tc.expr, tc.key, tc.blob)
			require.ErrorContains(t, err, "is not supported", tc.expr)
			require.ErrorContains(t, err, "membership_match", tc.expr)
		}
	})
}
