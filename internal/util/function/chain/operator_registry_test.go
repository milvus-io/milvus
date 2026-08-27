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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

type OperatorRegistryTestSuite struct {
	suite.Suite
}

func TestOperatorRegistryTestSuite(t *testing.T) {
	suite.Run(t, new(OperatorRegistryTestSuite))
}

func (s *OperatorRegistryTestSuite) TestRegisterOperatorEmptyType() {
	err := RegisterOperator("", func(_ *OperatorRepr, _ types.FunctionBuildContext) (Operator, error) { return nil, nil })
	s.Error(err)
	s.Contains(err.Error(), "cannot be empty")
}

func (s *OperatorRegistryTestSuite) TestRegisterOperatorNilFactory() {
	err := RegisterOperator("test_nil_factory", nil)
	s.Error(err)
	s.Contains(err.Error(), "cannot be nil")

	err = RegisterOperator("test_nil_stateless_factory", statelessOperatorFactory(nil))
	s.Error(err)
	s.Contains(err.Error(), "cannot be nil")
}

func (s *OperatorRegistryTestSuite) TestGetOperatorFactoryRegistered() {
	// Built-in contextual and stateless operators are registered uniformly.
	for _, opType := range []string{types.OpTypeMerge, types.OpTypeMap, types.OpTypeFilter, types.OpTypeSort} {
		factory, ok := GetOperatorFactory(opType)
		s.True(ok, opType)
		s.NotNil(factory, opType)
	}
}

func (s *OperatorRegistryTestSuite) TestGetOperatorFactoryNotRegistered() {
	factory, ok := GetOperatorFactory("nonexistent_operator_type")
	s.False(ok)
	s.Nil(factory)
}

func (s *OperatorRegistryTestSuite) TestMustRegisterOperatorPanics() {
	// Register a factory first
	err := RegisterOperator("test_must_panic", func(_ *OperatorRepr, _ types.FunctionBuildContext) (Operator, error) { return nil, nil })
	s.Require().NoError(err)

	// Registering again should panic
	s.Panics(func() {
		MustRegisterOperator("test_must_panic", func(_ *OperatorRepr, _ types.FunctionBuildContext) (Operator, error) { return nil, nil })
	})
}

func (s *OperatorRegistryTestSuite) TestRegisterOperatorDuplicate() {
	err := RegisterOperator("test_duplicate_reg", func(_ *OperatorRepr, _ types.FunctionBuildContext) (Operator, error) { return nil, nil })
	s.Require().NoError(err)

	err = RegisterOperator("test_duplicate_reg", func(_ *OperatorRepr, _ types.FunctionBuildContext) (Operator, error) { return nil, nil })
	s.Error(err)
	s.Contains(err.Error(), "already registered")
}

func (s *OperatorRegistryTestSuite) TestOperatorFactoryReceivesBuildContext() {
	const opType = "test_context_operator"
	want := &types.SearchRuntimeInfo{MetricTypes: []string{"COSINE"}}
	err := RegisterOperator(opType, func(_ *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
		s.Same(want, buildCtx.Search)
		return nil, nil
	})
	s.Require().NoError(err)

	factory, ok := GetOperatorFactory(opType)
	s.Require().True(ok)
	_, err = factory(&OperatorRepr{Type: opType}, types.FunctionBuildContext{Search: want})
	s.NoError(err)
}
