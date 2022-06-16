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

package msgstream

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

// unsubscribeChannels create consumer first, and unsubscribe channel through msgStream.close()
// TODO use streamnative pulsarctl
func UnsubscribeChannels(ctx context.Context, factory Factory, subName string, channels []string) {
	log.Info("unsubscribe channel", zap.String("subname", subName), zap.Any("channels", channels))
	err := factory.NewMsgStreamDisposer(ctx)(channels, subName)
	if err != nil {
		log.Warn("failed to unsubscribe channels", zap.String("subname", subName), zap.Any("channels", channels), zap.Error(err))
		panic(err)
	}
}
