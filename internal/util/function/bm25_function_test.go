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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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

func (s *BM25FunctionRunnerSuite) TestBM25() {
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
