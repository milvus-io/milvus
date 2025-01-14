/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quota

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestGetQuotaConfigMap(t *testing.T) {
	paramtable.Init()
	{
		m := GetQuotaConfigMap(internalpb.RateScope_Cluster)
		assert.Equal(t, 11, len(m))
	}
	{
		m := GetQuotaConfigMap(internalpb.RateScope_Database)
		assert.Equal(t, 11, len(m))
	}
	{
		m := GetQuotaConfigMap(internalpb.RateScope_Collection)
		assert.Equal(t, 7, len(m))
	}
	{
		m := GetQuotaConfigMap(internalpb.RateScope_Partition)
		assert.Equal(t, 6, len(m))
	}
	{
		m := GetQuotaConfigMap(internalpb.RateScope(1000))
		assert.Equal(t, 0, len(m))
	}
}

func TestGetQuotaValue(t *testing.T) {
	paramtable.Init()
	param := paramtable.Get()
	param.Save(param.QuotaConfig.DDLLimitEnabled.Key, "true")
	defer param.Reset(param.QuotaConfig.DDLLimitEnabled.Key)
	param.Save(param.QuotaConfig.DMLLimitEnabled.Key, "true")
	defer param.Reset(param.QuotaConfig.DMLLimitEnabled.Key)

	t.Run("cluster", func(t *testing.T) {
		param.Save(param.QuotaConfig.DDLCollectionRate.Key, "10")
		defer param.Reset(param.QuotaConfig.DDLCollectionRate.Key)
		v := GetQuotaValue(internalpb.RateScope_Cluster, internalpb.RateType_DDLCollection, param)
		assert.EqualValues(t, 10, v)
	})
	t.Run("database", func(t *testing.T) {
		param.Save(param.QuotaConfig.DDLCollectionRatePerDB.Key, "10")
		defer param.Reset(param.QuotaConfig.DDLCollectionRatePerDB.Key)
		v := GetQuotaValue(internalpb.RateScope_Database, internalpb.RateType_DDLCollection, param)
		assert.EqualValues(t, 10, v)
	})
	t.Run("collection", func(t *testing.T) {
		param.Save(param.QuotaConfig.DMLMaxInsertRatePerCollection.Key, "10")
		defer param.Reset(param.QuotaConfig.DMLMaxInsertRatePerCollection.Key)
		v := GetQuotaValue(internalpb.RateScope_Collection, internalpb.RateType_DMLInsert, param)
		assert.EqualValues(t, 10*1024*1024, v)
	})
	t.Run("partition", func(t *testing.T) {
		param.Save(param.QuotaConfig.DMLMaxInsertRatePerPartition.Key, "10")
		defer param.Reset(param.QuotaConfig.DMLMaxInsertRatePerPartition.Key)
		v := GetQuotaValue(internalpb.RateScope_Partition, internalpb.RateType_DMLInsert, param)
		assert.EqualValues(t, 10*1024*1024, v)
	})
	t.Run("unknown", func(t *testing.T) {
		v := GetQuotaValue(internalpb.RateScope(1000), internalpb.RateType(1000), param)
		assert.EqualValues(t, math.MaxFloat64, v)
	})
}
