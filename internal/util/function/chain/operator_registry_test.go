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
)

type OperatorRegistryTestSuite struct {
	suite.Suite
}

func TestOperatorRegistryTestSuite(t *testing.T) {
	suite.Run(t, new(OperatorRegistryTestSuite))
}

func (s *OperatorRegistryTestSuite) TestRegisterOperatorEmptyType() {
	err := RegisterOperator("", func(repr *OperatorRepr) (Operator, error) { return nil, nil })
	s.Error(err)
	s.Contains(err.Error(), "cannot be empty")
}

func (s *OperatorRegistryTestSuite) TestRegisterOperatorNilFactory() {
	err := RegisterOperator("test_nil_factory", nil)
	s.Error(err)
	s.Contains(err.Error(), "cannot be nil")
}

func (s *OperatorRegistryTestSuite) TestGetOperatorFactoryRegistered() {
	// Built-in operators registered via init() (map, filter, sort, etc.)
	factory, ok := GetOperatorFactory("map")
	s.True(ok)
	s.NotNil(factory)
}

func (s *OperatorRegistryTestSuite) TestGetOperatorFactoryNotRegistered() {
	factory, ok := GetOperatorFactory("nonexistent_operator_type")
	s.False(ok)
	s.Nil(factory)
}

func (s *OperatorRegistryTestSuite) TestMustRegisterOperatorPanics() {
	// Register a factory first
	err := RegisterOperator("test_must_panic", func(repr *OperatorRepr) (Operator, error) { return nil, nil })
	s.Require().NoError(err)

	// Registering again should panic
	s.Panics(func() {
		MustRegisterOperator("test_must_panic", func(repr *OperatorRepr) (Operator, error) { return nil, nil })
	})
}

func (s *OperatorRegistryTestSuite) TestRegisterOperatorDuplicate() {
	err := RegisterOperator("test_duplicate_reg", func(repr *OperatorRepr) (Operator, error) { return nil, nil })
	s.Require().NoError(err)

	err = RegisterOperator("test_duplicate_reg", func(repr *OperatorRepr) (Operator, error) { return nil, nil })
	s.Error(err)
	s.Contains(err.Error(), "already registered")
}
