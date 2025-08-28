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

package controllerimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/cdc/replication"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
)

func TestController_StartAndStop(t *testing.T) {
	mockReplicateManagerClient := replication.NewMockReplicateManagerClient(t)
	mockReplicateManagerClient.EXPECT().Close().Return()
	resource.InitForTest(t,
		resource.OptReplicateManagerClient(mockReplicateManagerClient),
	)

	ctrl := NewController()
	assert.NotPanics(t, func() {
		ctrl.Start()
	})
	assert.NotPanics(t, func() {
		ctrl.Stop()
	})
}

func TestController_Run(t *testing.T) {
	mockReplicateManagerClient := replication.NewMockReplicateManagerClient(t)
	mockReplicateManagerClient.EXPECT().Close().Return()

	testConfig := &commonpb.ReplicateConfiguration{}
	mockReplicationCatalog := mock_metastore.NewMockReplicationCatalog(t)
	mockReplicationCatalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(testConfig, nil)
	mockReplicateManagerClient.EXPECT().UpdateReplications(testConfig).Return()
	resource.InitForTest(t,
		resource.OptReplicateManagerClient(mockReplicateManagerClient),
		resource.OptReplicationCatalog(mockReplicationCatalog),
	)

	ctrl := NewController()
	ctrl.Start()
	defer ctrl.Stop()
	ctrl.run()
}

func TestController_RunError(t *testing.T) {
	mockReplicateManagerClient := replication.NewMockReplicateManagerClient(t)
	mockReplicateManagerClient.EXPECT().Close().Return()

	mockReplicationCatalog := mock_metastore.NewMockReplicationCatalog(t)
	mockReplicationCatalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, assert.AnError)
	resource.InitForTest(t,
		resource.OptReplicateManagerClient(mockReplicateManagerClient),
		resource.OptReplicationCatalog(mockReplicationCatalog),
	)

	ctrl := NewController()
	ctrl.Start()
	defer ctrl.Stop()
	ctrl.run()
}
