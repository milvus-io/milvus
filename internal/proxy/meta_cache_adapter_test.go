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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type MetaCacheCasbinAdapterSuite struct {
	suite.Suite

	cache   *MockCache
	adapter *MetaCacheCasbinAdapter
}

func (s *MetaCacheCasbinAdapterSuite) SetupTest() {
	s.cache = NewMockCache(s.T())

	s.adapter = NewMetaCacheCasbinAdapter(func() Cache { return s.cache })
}

func (s *MetaCacheCasbinAdapterSuite) TestLoadPolicy() {
	s.Run("normal_load", func() {
		s.cache.EXPECT().GetPrivilegeInfo(mock.Anything).Return([]string{})

		m := getPolicyModel(ModelStr)
		err := s.adapter.LoadPolicy(m)
		s.NoError(err)
	})

	s.Run("source_return_nil", func() {
		adapter := NewMetaCacheCasbinAdapter(func() Cache { return nil })

		m := getPolicyModel(ModelStr)
		err := adapter.LoadPolicy(m)
		s.Error(err)
	})
}

func (s *MetaCacheCasbinAdapterSuite) TestSavePolicy() {
	m := getPolicyModel(ModelStr)
	s.Error(s.adapter.SavePolicy(m))
}

func (s *MetaCacheCasbinAdapterSuite) TestAddPolicy() {
	s.Error(s.adapter.AddPolicy("", "", []string{}))
}

func (s *MetaCacheCasbinAdapterSuite) TestRemovePolicy() {
	s.Error(s.adapter.RemovePolicy("", "", []string{}))
}

func (s *MetaCacheCasbinAdapterSuite) TestRemoveFiltererPolicy() {
	s.Error(s.adapter.RemoveFilteredPolicy("", "", 0))
}

func TestMetaCacheCasbinAdapter(t *testing.T) {
	suite.Run(t, new(MetaCacheCasbinAdapterSuite))
}
