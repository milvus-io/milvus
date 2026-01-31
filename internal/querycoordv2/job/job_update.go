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

package job

import (
	"context"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type UpdateLoadConfigJob struct {
	*BaseJob
	collectionID               int64
	newReplicaNumber           int32
	newResourceGroups          []string
	newStreamingResourceGroups []string
	meta                       *meta.Meta
	targetMgr                  meta.TargetManagerInterface
	targetObserver             *observers.TargetObserver
	collectionObserver         *observers.CollectionObserver
	userSpecifiedReplicaMode   bool
}

func NewUpdateLoadConfigJob(ctx context.Context,
	req *querypb.UpdateLoadConfigRequest,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	targetObserver *observers.TargetObserver,
	collectionObserver *observers.CollectionObserver,
	userSpecifiedReplicaMode bool,
) *UpdateLoadConfigJob {
	collectionID := req.GetCollectionIDs()[0]
	return &UpdateLoadConfigJob{
		BaseJob:                    NewBaseJob(ctx, req.Base.GetMsgID(), collectionID),
		meta:                       meta,
		targetMgr:                  targetMgr,
		targetObserver:             targetObserver,
		collectionObserver:         collectionObserver,
		collectionID:               collectionID,
		newReplicaNumber:           req.GetReplicaNumber(),
		newResourceGroups:          req.GetResourceGroups(),
		newStreamingResourceGroups: req.GetStreamingResourceGroups(),
		userSpecifiedReplicaMode:   userSpecifiedReplicaMode,
	}
}

func (job *UpdateLoadConfigJob) Execute() error {
	if !job.meta.CollectionManager.Exist(job.ctx, job.collectionID) {
		msg := "modify replica for unloaded collection is not supported"
		err := merr.WrapErrCollectionNotLoaded(msg)
		log.Warn(msg, zap.Error(err))
		return err
	}

	// 1. check replica parameters
	if job.newReplicaNumber == 0 {
		msg := "set replica number to 0 for loaded collection is not supported"
		err := merr.WrapErrParameterInvalidMsg(msg)
		log.Warn(msg, zap.Error(err))
		return err
	}

	if len(job.newResourceGroups) == 0 {
		job.newResourceGroups = []string{meta.DefaultResourceGroupName}
	}

	// 2. Build target replica configs based on new replica number and resource groups
	channels := job.targetMgr.GetDmChannelsByCollection(job.ctx, job.collectionID, meta.CurrentTargetFirst)
	replicaConfigs := job.buildReplicaConfigs()

	log.Info("update load config",
		zap.Int64("collectionID", job.collectionID),
		zap.Int32("replicaNumber", job.newReplicaNumber),
		zap.Strings("resourceGroups", job.newResourceGroups),
		zap.Strings("streamingResourceGroups", job.newStreamingResourceGroups),
		zap.Int("targetReplicaCount", len(replicaConfigs)))

	// 3. Update replicas using UpdateWithReplicaConfig
	// This will match existing replicas by (ResourceGroup, StreamingResourceGroup),
	// reuse matched replicas, create new ones, and remove unmatched ones
	_, err := job.meta.ReplicaManager.UpdateWithReplicaConfig(job.ctx, meta.SpawnWithReplicaConfigParams{
		CollectionID: job.collectionID,
		Channels:     lo.Keys(channels),
		Configs:      replicaConfigs,
	})
	if err != nil {
		log.Warn("failed to update replicas", zap.Error(err))
		return err
	}

	// 4. recover node distribution among replicas
	utils.RecoverReplicaOfCollection(job.ctx, job.meta, job.collectionID)

	// 5. update replica number in meta
	err = job.meta.UpdateReplicaNumber(job.ctx, job.collectionID, job.newReplicaNumber, job.userSpecifiedReplicaMode)
	if err != nil {
		msg := "failed to update replica number"
		log.Warn(msg, zap.Error(err))
		return err
	}

	// 6. update next target, no need to rollback if pull target failed, target observer will pull target in periodically
	_, err = job.targetObserver.UpdateNextTarget(job.collectionID)
	if err != nil {
		msg := "failed to update next target"
		log.Warn(msg, zap.Error(err))
	}

	return nil
}

// buildReplicaConfigs builds LoadReplicaConfig array based on newReplicaNumber and resource groups
// Returns configs without ReplicaID set - IDs will be assigned by UpdateWithReplicaConfig
// Resource group assignment rules:
// - If len(resourceGroups) == 1: all replicas use this single resource group
// - If len(resourceGroups) == newReplicaNumber: replica i uses resourceGroups[i] (1-to-1 mapping in input order)
// Same rules apply to streamingResourceGroups
func (job *UpdateLoadConfigJob) buildReplicaConfigs() []*messagespb.LoadReplicaConfig {
	configs := make([]*messagespb.LoadReplicaConfig, 0, job.newReplicaNumber)

	singleRG := len(job.newResourceGroups) == 1
	singleStreamingRG := len(job.newStreamingResourceGroups) == 1

	for i := 0; i < int(job.newReplicaNumber); i++ {
		// Assign resource group
		rg := ""
		if singleRG {
			rg = job.newResourceGroups[0]
		} else {
			rg = job.newResourceGroups[i]
		}

		// Assign streaming resource group
		streamingRG := ""
		if len(job.newStreamingResourceGroups) > 0 {
			if singleStreamingRG {
				streamingRG = job.newStreamingResourceGroups[0]
			} else {
				streamingRG = job.newStreamingResourceGroups[i]
			}
		}

		config := &messagespb.LoadReplicaConfig{
			ReplicaId:                  0, // Not set, will be assigned by UpdateWithReplicaConfig
			ResourceGroupName:          rg,
			Priority:                   commonpb.LoadPriority_LOW,
			StreamingResourceGroupName: streamingRG,
		}
		configs = append(configs, config)
	}

	return configs
}
