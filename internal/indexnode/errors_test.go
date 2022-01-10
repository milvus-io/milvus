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

package indexnode

import (
	"testing"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestMsgIndexNodeIsUnhealthy(t *testing.T) {
	nodeIDList := []typeutil.UniqueID{1, 2, 3}
	for _, nodeID := range nodeIDList {
		log.Info("TestMsgIndexNodeIsUnhealthy", zap.String("msg", msgIndexNodeIsUnhealthy(nodeID)))
	}
}

func TestErrIndexNodeIsUnhealthy(t *testing.T) {
	nodeIDList := []typeutil.UniqueID{1, 2, 3}
	for _, nodeID := range nodeIDList {
		log.Info("TestErrIndexNodeIsUnhealthy", zap.Error(errIndexNodeIsUnhealthy(nodeID)))
	}
}
