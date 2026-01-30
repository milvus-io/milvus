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

package coordinator

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// RegisterWALCallbacks registers the WAL-related callbacks.
func RegisterWALCallbacks(s *mixCoordImpl) {
	walCallback := &WALCallback{
		mixCoord: s,
	}
	walCallback.registerAlterWALCallback()
}

// WALCallback is the callback for WAL-related operations.
type WALCallback struct {
	mixCoord *mixCoordImpl
}

// registerAlterWALCallback registers the alter WAL callback.
func (c *WALCallback) registerAlterWALCallback() {
	registry.RegisterAlterWALV2AckCallback(c.alterWALV2AckCallback)
}

// alterWALV2AckCallback is the callback function for AlterWAL message ack.
// This callback is called when all vchannels have acknowledged the AlterWAL broadcast message.
// It updates the etcd configuration mq.type to the targetWALName.
func (c *WALCallback) alterWALV2AckCallback(
	ctx context.Context,
	result message.BroadcastResult[*message.AlterWALMessageHeader, *message.AlterWALMessageBody],
) error {
	logger := log.Ctx(ctx).With(
		zap.Stringer("targetWALName", result.Message.Header().TargetWalName),
		zap.Any("config", result.Message.Header().Config),
		zap.Uint64("broadcastID", result.Message.BroadcastHeader().BroadcastID),
	)

	logger.Info("AlterWAL broadcast message acknowledged by all vchannels",
		zap.Int("vchannelCount", len(result.Results)))

	// Convert WALName enum to string representation
	targetWALName := result.Message.Header().TargetWalName
	mqTypeValue := message.WALName(targetWALName).String()
	if mqTypeValue == "unknown" {
		return errors.Errorf("invalid target WAL name: %v", targetWALName)
	}

	// Get etcd source to update configuration
	paramMgr := paramtable.GetBaseTable().Manager()
	etcdSource, ok := paramMgr.GetEtcdSource()
	if !ok {
		logger.Warn("failed to update mq.type config, etcd source not enabled")
		return errors.New("etcd source is not enabled, cannot update mq.type configuration")
	}

	// Update mq.type configuration in etcd
	configKey := paramtable.Get().MQCfg.Type.Key
	if err := paramMgr.UpdateConfigInEtcd(etcdSource, configKey, mqTypeValue); err != nil {
		logger.Error("failed to update mq.type config in etcd",
			zap.String("configKey", configKey),
			zap.String("mqTypeValue", mqTypeValue),
			zap.Error(err))
		return errors.Wrap(err, "failed to update mq.type configuration in etcd")
	}

	logger.Info("successfully updated mq.type configuration in etcd",
		zap.String("configKey", configKey),
		zap.String("mqTypeValue", mqTypeValue))

	return nil
}
