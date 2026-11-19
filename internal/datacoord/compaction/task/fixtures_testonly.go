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

//go:build test

package task

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NewDefaultMockCompactionMeta supplies empty group behavior for legacy tests.
func NewDefaultMockCompactionMeta(t *testing.T) *MockCompactionMeta {
	t.Helper()
	m := NewMockCompactionMeta(t)
	m.EXPECT().CompactionContext().Return(context.Background()).Maybe()
	m.EXPECT().GetCompactionChangeGroup(mock.Anything).Return((*model.SegmentChangeGroup)(nil), nil).Maybe()
	m.EXPECT().GetProducerChangeGroup(mock.Anything, mock.Anything, mock.Anything).Return((*model.SegmentChangeGroup)(nil)).Maybe()
	m.EXPECT().GetSegmentChangeGroupBySegmentID(mock.Anything, mock.Anything).Return((*model.SegmentChangeGroup)(nil)).Maybe()
	m.EXPECT().RecomputeCompactionDataView(mock.Anything, mock.Anything).Return().Maybe()
	m.EXPECT().UnpublishedSegmentMembers(mock.Anything, mock.Anything).Return(typeutil.NewUniqueSet()).Maybe()
	m.EXPECT().SegmentChangeL0Channels(mock.Anything).Return(typeutil.NewSet[string]()).Maybe()
	m.EXPECT().ReserveGroupSort(mock.Anything).Return(false).Maybe()
	return m
}

const Int64Field = "int64Field"

// newCompactionTaskMetaOverCatalog returns an empty compactionTaskMeta over catalog without reloading from it.
func NewCompactionTaskMetaOverCatalog(catalog metastore.DataCoordCatalog) *CompactionTaskMeta {
	return &CompactionTaskMeta{
		ctx:             context.Background(),
		catalog:         catalog,
		CompactionTasks: make(map[int64]map[int64]*datapb.CompactionTask),
		taskStats:       expirable.NewLRU[UniqueID, *metricsinfo.CompactionTask](512, nil, time.Minute*15),
	}
}

func NewTestCompactionTaskMeta(t *testing.T) *CompactionTaskMeta {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	meta, _ := NewCompactionTaskMeta(context.TODO(), catalog)
	return meta
}
