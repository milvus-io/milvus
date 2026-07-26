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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type MultiAnalyzerBM25FunctionSuite struct {
	suite.Suite

	collection *schemapb.CollectionSchema
	function   *schemapb.FunctionSchema
	runner     *MultiAnalyzerBM25FunctionRunner
}

func (s *MultiAnalyzerBM25FunctionSuite) SetupSuite() {
	s.collection = &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "text",
				FieldID:  101,
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "255",
					}, {
						Key:   "enable_analyzer",
						Value: "true",
					}, {
						Key:   "multi_analyzer_params",
						Value: "{\"by_field\": \"analyzer\", \"analyzers\": {\"default\": { \"type\": \"standard\"}, \"english\": {\"type\": \"english\"}}}",
					},
				},
			},
			{
				Name:     "analyzer",
				DataType: schemapb.DataType_VarChar,
				FieldID:  102,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "255",
					},
				},
			},
			{
				Name:     "output",
				FieldID:  103,
				DataType: schemapb.DataType_SparseFloatVector,
			},
		},
	}

	s.function = &schemapb.FunctionSchema{
		Name:             "bm25",
		Type:             schemapb.FunctionType_BM25,
		InputFieldNames:  []string{"text"},
		InputFieldIds:    []int64{101},
		OutputFieldNames: []string{"output"},
		OutputFieldIds:   []int64{103},
	}
}

func (s *MultiAnalyzerBM25FunctionSuite) TestNewMultiAnalyzerBM25FunctionRunner() {
	s.Run("normal", func() {
		runner, err := NewBM25FunctionRunner(s.collection, s.function)
		s.NoError(err)
		s.NotNil(runner)

		_, ok := runner.(*MultiAnalyzerBM25FunctionRunner)
		s.True(ok)
	})

	s.Run("lack dependent field in params", func() {
		// return error when by_field not in params
		_, err := NewMultiAnalyzerBM25FunctionRunner(s.collection, s.function, s.collection.Fields[0], s.collection.Fields[2], "{\"analyzers\": {\"default\": { \"type\": \"standard\"}}}")
		s.Error(err)
	})

	s.Run("dependent field name not string in params", func() {
		// return error when by_field not string
		_, err := NewMultiAnalyzerBM25FunctionRunner(s.collection, s.function, s.collection.Fields[0], s.collection.Fields[2], "\"by_field\": 1, {\"analyzers\": {\"default\": { \"type\": \"standard\"}}}")
		s.Error(err)
	})

	s.Run("dependent field not exist in collection", func() {
		_, err := NewMultiAnalyzerBM25FunctionRunner(s.collection, s.function, s.collection.Fields[0], s.collection.Fields[2], "{\"by_field\": \"not_exist\", \"analyzers\": {\"default\": { \"type\": \"standard\"}}}")
		s.Error(err)
	})

	s.Run("analyzers not exist in params", func() {
		// return error when analyzers not in params
		_, err := NewMultiAnalyzerBM25FunctionRunner(s.collection, s.function, s.collection.Fields[0], s.collection.Fields[2], "{\"by_field\": \"analyzer\"}")
		s.Error(err)
	})

	s.Run("analyzers not json object in params", func() {
		_, err := NewMultiAnalyzerBM25FunctionRunner(s.collection, s.function, s.collection.Fields[0], s.collection.Fields[2], "{\"by_field\": \"analyzer\", \"analyzers\": \"default\"}")
		s.Error(err)
	})

	s.Run("invalid analyzer in analyers", func() {
		_, err := NewMultiAnalyzerBM25FunctionRunner(s.collection, s.function, s.collection.Fields[0], s.collection.Fields[2], "{\"by_field\": \"analyzer\", \"analyzers\": {\"default\": { \"type\": \"invalid\"}}}")
		s.Error(err)
	})
}

func (s *MultiAnalyzerBM25FunctionSuite) TestConstructorRollsBackCreatedAnalyzersOnError() {
	created := interfaces.NewMockAnalyzer(s.T())
	created.EXPECT().Destroy().Return().Once()
	calls := 0
	patch := mockey.Mock(analyzer.NewAnalyzer).To(func(string, string) (analyzer.Analyzer, error) {
		calls++
		if calls == 1 {
			return created, nil
		}
		return nil, errors.New("create analyzer failed")
	}).Build()
	defer patch.UnPatch()

	runner, err := NewMultiAnalyzerBM25FunctionRunner(
		s.collection,
		s.function,
		s.collection.Fields[0],
		s.collection.Fields[2],
		`{"by_field":"analyzer","analyzers":{"default":{"type":"standard"},"english":{"type":"english"}}}`,
	)
	s.Nil(runner)
	s.ErrorContains(err, "create analyzer")
	s.Equal(2, calls)
}

func (s *MultiAnalyzerBM25FunctionSuite) TestBatchRun() {
	s.Run("normal", func() {
		runner, err := NewBM25FunctionRunner(s.collection, s.function)
		s.NoError(err)
		s.NotNil(runner)

		_, ok := runner.(*MultiAnalyzerBM25FunctionRunner)
		s.True(ok)

		// test batch run
		text := []string{"test of analyzer", "test of analyzer"}
		analyzerName := []string{"english", "default"}

		result, err := runner.BatchRun(text, analyzerName)
		s.NoError(err)

		sparseArray, ok := result[0].(*schemapb.SparseFloatArray)
		s.Require().True(ok)
		s.Require().Equal(2, len(sparseArray.GetContents()))

		// english analyzer will remove stop word like "of"
		// so the result will be two token
		// bytes size will be 2 * 2 * 4 = 16
		s.Equal(16, len(sparseArray.GetContents()[0]))
		// bytes size will be 3 * 2 * 4 = 24
		s.Equal(24, len(sparseArray.GetContents()[1]))

		runner.Close()

		// run after close
		_, err = runner.BatchRun(text, analyzerName)
		s.Error(err)
	})
}

func (s *MultiAnalyzerBM25FunctionSuite) TestBatchAnalyze() {
	s.Run("normal", func() {
		runner, err := NewBM25FunctionRunner(s.collection, s.function)
		s.NoError(err)
		s.NotNil(runner)

		analyzer, ok := runner.(Analyzer)
		s.True(ok)

		text := []string{"test of analyzer", "test of analyzer"}
		analyzerName := []string{"english", "default"}

		result, err := analyzer.BatchAnalyze(true, false, text, analyzerName)
		s.NoError(err)

		s.Equal(2, len(result))
		s.Equal(2, len(result[0]))
		s.Equal(3, len(result[1]))
	})
}

func (s *MultiAnalyzerBM25FunctionSuite) newTrackingAnalyzer(active *atomic.Int32, maxActive *atomic.Int32) *interfaces.MockAnalyzer {
	tokenizer := interfaces.NewMockAnalyzer(s.T())
	tokenizer.EXPECT().Clone().Return(tokenizer, nil)
	tokenizer.EXPECT().NewTokenStream(mock.Anything).RunAndReturn(func(text string) interfaces.TokenStream {
		return newTrackingTokenStream(text, active, maxActive)
	})
	tokenizer.EXPECT().Destroy().Return()
	return tokenizer
}

func (s *MultiAnalyzerBM25FunctionSuite) TestRunReleasesTokenStreamsPerInput() {
	var active, maxActive atomic.Int32
	runner := &MultiAnalyzerBM25FunctionRunner{
		analyzers: map[string]analyzer.Analyzer{
			"default": s.newTrackingAnalyzer(&active, &maxActive),
		},
	}
	dst := make([]map[uint32]float32, 3)

	err := runner.run([]string{"a", "b", "c"}, []string{"default", "default", "default"}, dst)

	s.NoError(err)
	s.Equal(int32(0), active.Load())
	s.Equal(int32(1), maxActive.Load())
}

func (s *MultiAnalyzerBM25FunctionSuite) TestAnalyzeReleasesTokenStreamsPerInput() {
	var active, maxActive atomic.Int32
	runner := &MultiAnalyzerBM25FunctionRunner{
		analyzers: map[string]analyzer.Analyzer{
			"default": s.newTrackingAnalyzer(&active, &maxActive),
		},
	}
	dst := make([][]*milvuspb.AnalyzerToken, 3)

	err := runner.analyze([]string{"a", "b", "c"}, []string{"default", "default", "default"}, dst, false, false)

	s.NoError(err)
	s.Equal(int32(0), active.Load())
	s.Equal(int32(1), maxActive.Load())
}

func TestMultiAnalyzerBm25Function(t *testing.T) {
	suite.Run(t, new(MultiAnalyzerBM25FunctionSuite))
}

// getAnalyzer must terminate with an error — not recurse forever — when even the
// "default" analyzer cannot resolve (missing, or aliased to an undefined name).
func TestMultiAnalyzerGetAnalyzerTerminalGuard(t *testing.T) {
	t.Run("dangling default alias returns error instead of recursing", func(t *testing.T) {
		runner := &MultiAnalyzerBM25FunctionRunner{
			alias:     map[string]string{"default": "missing"},
			analyzers: map[string]analyzer.Analyzer{},
		}
		_, err := runner.getAnalyzer("en", map[string]analyzer.Analyzer{})
		require.Error(t, err)
		require.ErrorContains(t, err, "default analyzer is missing or aliased to an undefined analyzer")
		// broken persisted configuration, not request content: must classify as
		// a system-side function failure, not an input error
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
	})

	t.Run("unknown name falls back to existing default", func(t *testing.T) {
		def := interfaces.NewMockAnalyzer(t)
		def.EXPECT().Clone().Return(def, nil).Once()
		runner := &MultiAnalyzerBM25FunctionRunner{
			alias:     map[string]string{"en": "english_typo"},
			analyzers: map[string]analyzer.Analyzer{"default": def},
		}
		got, err := runner.getAnalyzer("en", map[string]analyzer.Analyzer{})
		require.NoError(t, err)
		require.NotNil(t, got)
	})

	t.Run("missing default without alias returns error", func(t *testing.T) {
		runner := &MultiAnalyzerBM25FunctionRunner{
			analyzers: map[string]analyzer.Analyzer{},
		}
		_, err := runner.getAnalyzer("default", map[string]analyzer.Analyzer{})
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
	})
}
