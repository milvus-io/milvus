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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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
	})
}

func TestMultiAnalyzerBm25Function(t *testing.T) {
	suite.Run(t, new(MultiAnalyzerBM25FunctionSuite))
}
