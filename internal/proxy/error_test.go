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

package proxy

import (
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

func TestMsgProxyIsUnhealthy(t *testing.T) {
	ids := []UniqueID{
		1,
	}

	for _, id := range ids {
		log.Info("TestMsgProxyIsUnhealthy",
			zap.String("msg", msgProxyIsUnhealthy(id)))
	}
}

func TestErrProxyIsUnhealthy(t *testing.T) {
	ids := []UniqueID{
		1,
	}

	for _, id := range ids {
		log.Info("TestErrProxyIsUnhealthy",
			zap.Error(errProxyIsUnhealthy(id)))
	}
}
