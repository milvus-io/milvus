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

type SelectOpTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *SelectOpTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *SelectOpTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestSelectOpTestSuite(t *testing.T) {
	suite.Run(t, new(SelectOpTestSuite))
}

func (s *SelectOpTestSuite) createThreeColumnDF() *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{3})

	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()

	scoreBuilder := array.NewFloat64Builder(s.pool)
	scoreBuilder.AppendValues([]float64{0.9, 0.8, 0.7}, nil)
	scoreChunk := scoreBuilder.NewArray()
	scoreBuilder.Release()

	nameBuilder := array.NewStringBuilder(s.pool)
	nameBuilder.AppendValues([]string{"a", "b", "c"}, nil)
	nameChunk := nameBuilder.NewArray()
	nameBuilder.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreChunk})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks("name", []arrow.Array{nameChunk})
	s.Require().NoError(err)

	return builder.Build()
}

func (s *SelectOpTestSuite) TestSelectSubset() {
	df := s.createThreeColumnDF()
	defer df.Release()

	op := NewSelectOp([]string{types.IDFieldName, types.ScoreFieldName})
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())
	s.NotNil(result.Column(types.IDFieldName))
	s.NotNil(result.Column(types.ScoreFieldName))
	s.Nil(result.Column("name"))
}

func (s *SelectOpTestSuite) TestSelectColumnNotFound() {
	df := s.createThreeColumnDF()
	defer df.Release()

	op := NewSelectOp([]string{"nonexistent"})
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "not found")
}

func (s *SelectOpTestSuite) TestSelectAllColumns() {
	df := s.createThreeColumnDF()
	defer df.Release()

	op := NewSelectOp([]string{types.IDFieldName, types.ScoreFieldName, "name"})
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())
	s.NotNil(result.Column(types.IDFieldName))
	s.NotNil(result.Column(types.ScoreFieldName))
	s.NotNil(result.Column("name"))
}

func (s *SelectOpTestSuite) TestSelectDuplicateColumns() {
	df := s.createThreeColumnDF()
	defer df.Release()

	// Selecting same column twice should fail (duplicate column names)
	op := NewSelectOp([]string{types.IDFieldName, types.IDFieldName})
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "already exists")
}
