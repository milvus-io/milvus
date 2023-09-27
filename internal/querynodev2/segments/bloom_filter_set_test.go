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

package segments

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/storage"
)

type BloomFilterSetSuite struct {
	suite.Suite

	intPks    []int64
	stringPks []string
	set       *bloomFilterSet
}

func (suite *BloomFilterSetSuite) SetupTest() {
	suite.intPks = []int64{1, 2, 3}
	suite.stringPks = []string{"1", "2", "3"}
	suite.set = newBloomFilterSet()
}

func (suite *BloomFilterSetSuite) TestInt64PkBloomFilter() {
	pks, err := storage.GenInt64PrimaryKeys(suite.intPks...)
	suite.NoError(err)

	suite.set.UpdateBloomFilter(pks)
	for _, pk := range pks {
		exist := suite.set.MayPkExist(pk)
		suite.True(exist)
	}
}

func (suite *BloomFilterSetSuite) TestStringPkBloomFilter() {
	pks, err := storage.GenVarcharPrimaryKeys(suite.stringPks...)
	suite.NoError(err)

	suite.set.UpdateBloomFilter(pks)
	for _, pk := range pks {
		exist := suite.set.MayPkExist(pk)
		suite.True(exist)
	}
}

func (suite *BloomFilterSetSuite) TestHistoricalBloomFilter() {
	pks, err := storage.GenVarcharPrimaryKeys(suite.stringPks...)
	suite.NoError(err)

	suite.set.UpdateBloomFilter(pks)
	for _, pk := range pks {
		exist := suite.set.MayPkExist(pk)
		suite.True(exist)
	}

	old := suite.set.currentStat
	suite.set.currentStat = nil
	for _, pk := range pks {
		exist := suite.set.MayPkExist(pk)
		suite.False(exist)
	}

	suite.set.AddHistoricalStats(old)
	for _, pk := range pks {
		exist := suite.set.MayPkExist(pk)
		suite.True(exist)
	}
}

func TestBloomFilterSet(t *testing.T) {
	suite.Run(t, &BloomFilterSetSuite{})
}
