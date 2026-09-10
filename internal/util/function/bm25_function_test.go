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

package function

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestBM25FunctionRunnerSuite(t *testing.T) {
	suite.Run(t, new(BM25FunctionRunnerSuite))
}

type BM25FunctionRunnerSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *BM25FunctionRunnerSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
	}
}

func (s *BM25FunctionRunnerSuite) TestBatchRun() {
	_, err := NewFunctionRunner(s.schema, &schemapb.FunctionSchema{
		Name:          "test",
		Type:          schemapb.FunctionType_BM25,
		InputFieldIds: []int64{101},
	})
	s.Error(err)

	runner, err := NewFunctionRunner(s.schema, &schemapb.FunctionSchema{
		Name:           "test",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	})

	s.NoError(err)

	// test batch function run
	output, err := runner.BatchRun([]string{"test string", "test string 2"})
	s.NoError(err)

	s.Equal(1, len(output))
	result, ok := output[0].(*schemapb.SparseFloatArray)
	s.True(ok)
	s.Equal(2, len(result.GetContents()))

	// return error because receive more than one field input
	_, err = runner.BatchRun([]string{}, []string{})
	s.Error(err)

	// return error because field not string
	_, err = runner.BatchRun([]int64{})
	s.Error(err)

	runner.Close()

	// run after close
	_, err = runner.BatchRun([]string{"test string", "test string 2"})
	s.Error(err)
}

func (s *BM25FunctionRunnerSuite) TestBatchAnalyze() {
	runner, err := NewFunctionRunner(s.schema, &schemapb.FunctionSchema{
		Name:           "test",
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	})
	s.NoError(err)

	analyzer, ok := runner.(Analyzer)
	s.True(ok)

	result, err := analyzer.BatchAnalyze(true, false, []string{"test string", "test string 2"})
	s.NoError(err)

	s.Equal(2, len(result))
	s.Equal(2, len(result[0]))
	s.Equal(3, len(result[1]))
}

type singleTokenStream struct {
	token   string
	advance bool
}

func (s *singleTokenStream) Advance() bool {
	if s.advance {
		return false
	}
	s.advance = true
	return true
}

func (s *singleTokenStream) Token() string {
	return s.token
}

func (s *singleTokenStream) DetailedToken() *milvuspb.AnalyzerToken {
	return &milvuspb.AnalyzerToken{Token: s.token}
}

func (s *singleTokenStream) Destroy() {}

type trackingTokenStream struct {
	singleTokenStream
	active    *atomic.Int32
	maxActive *atomic.Int32
}

func newTrackingTokenStream(token string, active *atomic.Int32, maxActive *atomic.Int32) *trackingTokenStream {
	current := active.Add(1)
	for {
		max := maxActive.Load()
		if current <= max || maxActive.CompareAndSwap(max, current) {
			break
		}
	}
	return &trackingTokenStream{
		singleTokenStream: singleTokenStream{token: token},
		active:            active,
		maxActive:         maxActive,
	}
}

func (s *trackingTokenStream) Destroy() {
	s.active.Add(-1)
}

func (s *BM25FunctionRunnerSuite) newCloneCountingAnalyzer(cloneCount *atomic.Int32) *interfaces.MockAnalyzer {
	tokenizer := interfaces.NewMockAnalyzer(s.T())
	tokenizer.EXPECT().Clone().RunAndReturn(func() (interfaces.Analyzer, error) {
		cloneCount.Add(1)
		return tokenizer, nil
	})
	tokenizer.EXPECT().NewTokenStream(mock.Anything).RunAndReturn(func(text string) interfaces.TokenStream {
		return &singleTokenStream{token: text}
	})
	tokenizer.EXPECT().Destroy().Return()
	return tokenizer
}

func (s *BM25FunctionRunnerSuite) newTrackingAnalyzer(active *atomic.Int32, maxActive *atomic.Int32) *interfaces.MockAnalyzer {
	tokenizer := interfaces.NewMockAnalyzer(s.T())
	tokenizer.EXPECT().Clone().Return(tokenizer, nil)
	tokenizer.EXPECT().NewTokenStream(mock.Anything).RunAndReturn(func(text string) interfaces.TokenStream {
		return newTrackingTokenStream(text, active, maxActive)
	})
	tokenizer.EXPECT().Destroy().Return()
	return tokenizer
}

func (s *BM25FunctionRunnerSuite) TestRunReleasesTokenStreamsPerInput() {
	var active, maxActive atomic.Int32
	runner := &BM25FunctionRunner{tokenizer: s.newTrackingAnalyzer(&active, &maxActive)}
	dst := make([]map[uint32]float32, 3)

	err := runner.run([]string{"a", "b", "c"}, dst)

	s.NoError(err)
	s.Equal(int32(0), active.Load())
	s.Equal(int32(1), maxActive.Load())
}

func (s *BM25FunctionRunnerSuite) TestAnalyzeReleasesTokenStreamsPerInput() {
	var active, maxActive atomic.Int32
	runner := &BM25FunctionRunner{tokenizer: s.newTrackingAnalyzer(&active, &maxActive)}
	dst := make([][]*milvuspb.AnalyzerToken, 3)

	err := runner.analyze([]string{"a", "b", "c"}, dst, false, false)

	s.NoError(err)
	s.Equal(int32(0), active.Load())
	s.Equal(int32(1), maxActive.Load())
}

func (s *BM25FunctionRunnerSuite) TestAnalyzerRunnerConcurrencyConfigDynamic() {
	cfg := &paramtable.Get().FunctionCfg.AnalyzerRunnerConcurrency
	old := cfg.SwapTempValue("2")
	defer cfg.SwapTempValue(old)

	var cloneCount atomic.Int32
	tokenizer := s.newCloneCountingAnalyzer(&cloneCount)
	runner := &BM25FunctionRunner{tokenizer: tokenizer}
	input := []string{"a", "b", "c", "d", "e", "f"}

	_, err := runner.BatchRun(input)
	s.Require().NoError(err)
	s.Equal(int32(2), cloneCount.Load())

	cfg.SwapTempValue("3")

	_, err = runner.BatchRun(input)
	s.Require().NoError(err)
	s.Equal(int32(5), cloneCount.Load())
}

func (s *BM25FunctionRunnerSuite) TestResizeAnalyzerPool() {
	cfg := &paramtable.Get().FunctionCfg.AnalyzerConcurrencyPerCPUCore
	old := cfg.SwapTempValue("1")
	defer func() {
		cfg.SwapTempValue(old)
		ResizeAnalyzerPool(&config.Event{HasUpdated: true})
	}()

	pool := getOrCreateAnalyzerPool()
	ResizeAnalyzerPool(&config.Event{HasUpdated: true})
	s.Equal(hardware.GetCPUNum(), pool.Cap())

	cfg.SwapTempValue("2")
	ResizeAnalyzerPool(&config.Event{HasUpdated: true})
	s.Equal(hardware.GetCPUNum()*2, pool.Cap())
}
