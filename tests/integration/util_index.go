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

package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
)

const (
	IndexRaftIvfFlat     = indexparamcheck.IndexRaftIvfFlat
	IndexRaftIvfPQ       = indexparamcheck.IndexRaftIvfPQ
	IndexFaissIDMap      = indexparamcheck.IndexFaissIDMap
	IndexFaissIvfFlat    = indexparamcheck.IndexFaissIvfFlat
	IndexFaissIvfPQ      = indexparamcheck.IndexFaissIvfPQ
	IndexFaissIvfSQ8     = indexparamcheck.IndexFaissIvfSQ8
	IndexFaissBinIDMap   = indexparamcheck.IndexFaissBinIDMap
	IndexFaissBinIvfFlat = indexparamcheck.IndexFaissBinIvfFlat
	IndexHNSW            = indexparamcheck.IndexHNSW
	IndexDISKANN         = indexparamcheck.IndexDISKANN
)

func (s *MiniClusterSuite) WaitForIndexBuilt(ctx context.Context, collection, field string) {
	getIndexBuilt := func() bool {
		resp, err := s.Cluster.Proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			CollectionName: collection,
			FieldName:      field,
		})
		if err != nil {
			s.FailNow("failed to describe index")
			return true
		}
		for _, desc := range resp.GetIndexDescriptions() {
			if desc.GetFieldName() == field {
				switch desc.GetState() {
				case commonpb.IndexState_Finished:
					return true
				case commonpb.IndexState_Failed:
					return false
				}
			}
		}
		return false
	}
	for !getIndexBuilt() {
		select {
		case <-ctx.Done():
			s.FailNow("failed to wait index built until ctx done")
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func waitingForIndexBuilt(ctx context.Context, cluster *MiniCluster, t *testing.T, collection, field string) {
	getIndexBuilt := func() bool {
		resp, err := cluster.Proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			CollectionName: collection,
			FieldName:      field,
		})
		if err != nil {
			t.FailNow()
			return true
		}
		for _, desc := range resp.GetIndexDescriptions() {
			if desc.GetFieldName() == field {
				switch desc.GetState() {
				case commonpb.IndexState_Finished:
					return true
				case commonpb.IndexState_Failed:
					return false
				}
			}
		}
		return false
	}
	for !getIndexBuilt() {
		select {
		case <-ctx.Done():
			t.FailNow()
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func ConstructIndexParam(dim int, indexType string, metricType string) []*commonpb.KeyValuePair {
	params := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(dim),
		},
		{
			Key:   common.MetricTypeKey,
			Value: metricType,
		},
		{
			Key:   common.IndexTypeKey,
			Value: indexType,
		},
	}
	switch indexType {
	case IndexFaissIDMap, IndexFaissBinIDMap:
	// no index param is required
	case IndexFaissIvfFlat, IndexFaissBinIvfFlat, IndexFaissIvfSQ8:
		params = append(params, &commonpb.KeyValuePair{
			Key:   "nlist",
			Value: "100",
		})
	case IndexFaissIvfPQ:
		params = append(params, &commonpb.KeyValuePair{
			Key:   "nlist",
			Value: "100",
		})
		params = append(params, &commonpb.KeyValuePair{
			Key:   "m",
			Value: "16",
		})
		params = append(params, &commonpb.KeyValuePair{
			Key:   "nbits",
			Value: "8",
		})
	case IndexHNSW:
		params = append(params, &commonpb.KeyValuePair{
			Key:   "M",
			Value: "16",
		})
		params = append(params, &commonpb.KeyValuePair{
			Key:   "efConstruction",
			Value: "200",
		})
	case IndexDISKANN:
	default:
		panic(fmt.Sprintf("unimplemented index param for %s, please help to improve it", indexType))
	}
	return params
}

func GetSearchParams(indexType string, metricType string) map[string]any {
	params := make(map[string]any)
	switch indexType {
	case IndexFaissIDMap, IndexFaissBinIDMap:
		params[common.MetricTypeKey] = metricType
	case IndexFaissIvfFlat, IndexFaissBinIvfFlat, IndexFaissIvfSQ8, IndexFaissIvfPQ:
		params["nprobe"] = 8
	case IndexHNSW:
		params["ef"] = 200
	case IndexDISKANN:
		params["search_list"] = 5
	default:
		panic(fmt.Sprintf("unimplemented search param for %s, please help to improve it", indexType))
	}
	return params
}
