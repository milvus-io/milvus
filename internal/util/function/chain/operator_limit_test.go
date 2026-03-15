/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

type LimitOpTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *LimitOpTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *LimitOpTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestLimitOpTestSuite(t *testing.T) {
	suite.Run(t, new(LimitOpTestSuite))
}

func (s *LimitOpTestSuite) createLimitTestDF(ids []int64, chunkSizes []int64) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)

	offset := 0
	idChunks := make([]arrow.Array, len(chunkSizes))
	for i, size := range chunkSizes {
		idBuilder := array.NewInt64Builder(s.pool)
		for j := 0; j < int(size); j++ {
			idBuilder.Append(ids[offset+j])
		}
		idChunks[i] = idBuilder.NewArray()
		idBuilder.Release()
		offset += int(size)
	}

	err := builder.AddColumnFromChunks(types.IDFieldName, idChunks)
	s.Require().NoError(err)
	return builder.Build()
}

func (s *LimitOpTestSuite) TestLimitBasic() {
	df := s.createLimitTestDF([]int64{1, 2, 3, 4, 5}, []int64{5})
	defer df.Release()

	op := NewLimitOp(3, 0)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())
	idCol := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	s.Equal(int64(1), idCol.Value(0))
	s.Equal(int64(2), idCol.Value(1))
	s.Equal(int64(3), idCol.Value(2))
}

func (s *LimitOpTestSuite) TestLimitWithOffset() {
	df := s.createLimitTestDF([]int64{1, 2, 3, 4, 5}, []int64{5})
	defer df.Release()

	op := NewLimitOp(2, 2)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(2), result.NumRows())
	idCol := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	s.Equal(int64(3), idCol.Value(0))
	s.Equal(int64(4), idCol.Value(1))
}

func (s *LimitOpTestSuite) TestLimitOffsetExceedsChunkSize() {
	df := s.createLimitTestDF([]int64{1, 2, 3}, []int64{3})
	defer df.Release()

	// Offset larger than chunk size - should return 0 rows
	op := NewLimitOp(10, 100)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(0), result.NumRows())
}

func (s *LimitOpTestSuite) TestLimitLargerThanData() {
	df := s.createLimitTestDF([]int64{1, 2}, []int64{2})
	defer df.Release()

	// Limit larger than data - should return all rows
	op := NewLimitOp(100, 0)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(2), result.NumRows())
}

func (s *LimitOpTestSuite) TestLimitMultiChunk() {
	// Two chunks, limit applied independently
	df := s.createLimitTestDF([]int64{1, 2, 3, 4, 5, 6}, []int64{3, 3})
	defer df.Release()

	op := NewLimitOp(2, 0)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// Each chunk limited to 2
	s.Equal(int64(4), result.NumRows())
	ids0 := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	s.Equal(int64(1), ids0.Value(0))
	s.Equal(int64(2), ids0.Value(1))
	ids1 := result.Column(types.IDFieldName).Chunk(1).(*array.Int64)
	s.Equal(int64(4), ids1.Value(0))
	s.Equal(int64(5), ids1.Value(1))
}

func (s *LimitOpTestSuite) TestLimitEmptyChunk() {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{0})

	idBuilder := array.NewInt64Builder(s.pool)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk})
	s.Require().NoError(err)
	df := builder.Build()
	defer df.Release()

	op := NewLimitOp(10, 0)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(0), result.NumRows())
}
