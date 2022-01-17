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

package indexcoord

import (
	"testing"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

func TestErrIndexNodeIsNotOnService(t *testing.T) {
	indexNodeIDList := []UniqueID{
		1,
	}

	for _, id := range indexNodeIDList {
		log.Info("TestErrIndexNodeIsNotOnService",
			zap.Error(errIndexNodeIsNotOnService(id)))
	}
}

func TestMsgIndexCoordIsUnhealthy(t *testing.T) {
	nodeIDList := []UniqueID{1, 2, 3}
	for _, nodeID := range nodeIDList {
		log.Info("TestMsgIndexCoordIsUnhealthy", zap.String("msg", msgIndexCoordIsUnhealthy(nodeID)))
	}
}

func TestErrIndexCoordIsUnhealthy(t *testing.T) {
	nodeIDList := []UniqueID{1, 2, 3}
	for _, nodeID := range nodeIDList {
		log.Info("TestErrIndexCoordIsUnhealthy", zap.Error(errIndexCoordIsUnhealthy(nodeID)))
	}
}
