// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type LoadInfo struct {
	collectionID  int64
	loadFieldList []int64
	loadFieldSet  typeutil.Set[int64]
}

func NewLoadInfo(collectionID int64, loadedFields []int64) *LoadInfo {
	info := &LoadInfo{
		collectionID:  collectionID,
		loadFieldList: loadedFields,
		loadFieldSet:  typeutil.NewSet(loadedFields...),
	}
	return info
}

func (li *LoadInfo) IsFieldLoaded(fieldID int64) bool {
	// legacy behavior, no field partial loaded, assume all fields are loaded
	if len(li.loadFieldList) == 0 {
		return true
	}
	return li.loadFieldSet.Contain(fieldID)
}

func (li *LoadInfo) GetLoadedFields() []int64 {
	return li.loadFieldList
}

type LoadInfoCache interface {
	GetLoadInfo(ctx context.Context, collectionID int64) (*LoadInfo, error)
	Expire(ctx context.Context, collectionID int64)
}

type loadInfoCache struct {
	loadInfos *typeutil.ConcurrentMap[int64, *LoadInfo]

	queryCoord types.QueryCoordClient
	sf         conc.Singleflight[*LoadInfo]
}

func NewLoadInfoCache(qc types.QueryCoordClient) *loadInfoCache {
	return &loadInfoCache{
		loadInfos: typeutil.NewConcurrentMap[int64, *LoadInfo](),

		queryCoord: qc,
	}
}

func (c *loadInfoCache) GetLoadInfo(ctx context.Context, collectionID int64) (*LoadInfo, error) {
	info, ok := c.loadInfos.Get(collectionID)
	if ok {
		return info, nil
	}

	info, err, _ := c.sf.Do(fmt.Sprintf("%d", collectionID), func() (loadInfo *LoadInfo, err error) {
		var loadedFields []int64
		loadedFields, err = c.getCollectionLoadFields(ctx, collectionID)
		if err != nil {
			return nil, err
		}
		loadInfo = NewLoadInfo(collectionID, loadedFields)
		c.loadInfos.Insert(collectionID, loadInfo)
		return loadInfo, nil
	})
	return info, err
}

func (c *loadInfoCache) Expire(ctx context.Context, collectionID int64) {
	c.loadInfos.Remove(collectionID)
}

func (c *loadInfoCache) getCollectionLoadFields(ctx context.Context, collectionID UniqueID) ([]int64, error) {
	req := &querypb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionIDs: []int64{collectionID},
	}

	resp, err := c.queryCoord.ShowCollections(ctx, req)
	if err != nil {
		return nil, err
	}
	// backward compatility, ignore HPL logic
	if len(resp.GetLoadFields()) < 1 {
		return []int64{}, nil
	}
	return resp.GetLoadFields()[0].GetData(), nil
}
