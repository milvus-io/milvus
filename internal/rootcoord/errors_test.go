// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rootcoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

func TestMsgRootCoordIsUnhealthy(t *testing.T) {
	nodeIDList := []typeutil.UniqueID{1, 2, 3}
	for _, nodeID := range nodeIDList {
		assert.NotEmpty(t, msgRootCoordIsUnhealthy(nodeID))
		log.Info("TestMsgRootCoordIsUnhealthy", zap.String("msg", msgRootCoordIsUnhealthy(nodeID)))
	}
}

func TestErrRootCoordIsUnhealthy(t *testing.T) {
	nodeIDList := []typeutil.UniqueID{1, 2, 3}
	for _, nodeID := range nodeIDList {
		assert.NotNil(t, errRootCoordIsUnhealthy(nodeID))
		log.Info("TestErrRootCoordIsUnhealthy", zap.Error(errRootCoordIsUnhealthy(nodeID)))
	}
}
