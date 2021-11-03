// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"testing"

	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/gogo/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

func TestIsSameKVPair(t *testing.T) {
	key1 := funcutil.GenRandomStr()
	value1 := funcutil.GenRandomStr()

	key2 := key1
	value2 := value1

	key3 := key1 + funcutil.GenRandomStr()
	value3 := value1 + funcutil.GenRandomStr()

	pair1 := &commonpb.KeyValuePair{
		Key:   key1,
		Value: value1,
	}
	pair2 := &commonpb.KeyValuePair{
		Key:   key2,
		Value: value2,
	}
	pair3 := &commonpb.KeyValuePair{
		Key:   key3,
		Value: value3,
	}

	assert.True(t, isSameKVPair(pair1, pair2))
	assert.False(t, isSameKVPair(pair1, pair3))
	assert.False(t, isSameKVPair(pair2, pair3))

	assert.True(t, isSameKVPairWrapper(pair1, pair2))
	assert.False(t, isSameKVPairWrapper(pair1, pair3))
	assert.False(t, isSameKVPairWrapper(pair2, pair3))
}

func TestCanBeMerged(t *testing.T) {
	reqsWithoutPlg := &milvuspb.SearchRequest{
		Base:             nil,
		DbName:           funcutil.GenRandomStr(),
		CollectionName:   funcutil.GenRandomStr(),
		PartitionNames:   []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
		Dsl:              funcutil.GenRandomStr(),
		PlaceholderGroup: nil,
		DslType:          commonpb.DslType_BoolExprV1,
		OutputFields:     []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
		SearchParams: []*commonpb.KeyValuePair{
			{
				Key:   funcutil.GenRandomStr(),
				Value: funcutil.GenRandomStr(),
			},
			{
				Key:   funcutil.GenRandomStr(),
				Value: funcutil.GenRandomStr(),
			},
		},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
		Merged:             false,
		Nqs:                nil,
		MsgIds:             nil,
	}

	assert.True(t, canBeMerged(true, true, reqsWithoutPlg, reqsWithoutPlg))

	reqsWithoutPlgCopy := proto.Clone(reqsWithoutPlg).(*milvuspb.SearchRequest)
	assert.True(t, canBeMerged(true, true, reqsWithoutPlg, reqsWithoutPlgCopy))
}

func TestMergeMultipleSearchRequests(t *testing.T) {
	var err error

	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	expr := funcutil.GenRandomStr()
	floatVecField := funcutil.GenRandomStr()

	// not important though
	nprobe := 10
	topk := 5
	roundDecimal := 6

	nq1 := 10
	nq2 := 20

	dim := 16

	id1 := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	id2 := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())

	req1 := constructSearchRequest(dbName, collectionName, expr, floatVecField,
		nq1, dim, nprobe, topk, roundDecimal)
	req2 := constructSearchRequest(dbName, collectionName, expr, floatVecField,
		nq2, dim, nprobe, topk, roundDecimal)
	req1.Base = &commonpb.MsgBase{
		MsgID: id1,
	}
	req2.Base = &commonpb.MsgBase{
		MsgID: id2,
	}
	assert.True(t, canBeMerged(true, true, req1, req2))

	merged := mergeMultipleSearchRequests(req1, req2)
	assert.NotNil(t, merged)

	assert.ElementsMatch(t, merged.MsgIds, []UniqueID{id1, id2})
	assert.ElementsMatch(t, merged.Nqs, []int64{int64(nq1), int64(nq2)})

	var placeholderGroup milvuspb.PlaceholderGroup
	err = proto.Unmarshal(merged.PlaceholderGroup, &placeholderGroup)
	assert.NoError(t, err)
	assert.Equal(t, len(placeholderGroup.Placeholders[0].Values), nq1+nq2)
}
