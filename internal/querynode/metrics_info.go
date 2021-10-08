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

package querynode

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, node *QueryNode) (*milvuspb.GetMetricsResponse, error) {
	nodeInfos := metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeID),
			HardwareInfos: metricsinfo.HardwareMetrics{
				IP:           node.session.Address,
				CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
				CPUCoreUsage: metricsinfo.GetCPUUsage(),
				Memory:       metricsinfo.GetMemoryCount(),
				MemoryUsage:  metricsinfo.GetUsedMemoryCount(),
				Disk:         metricsinfo.GetDiskCount(),
				DiskUsage:    metricsinfo.GetDiskUsage(),
			},
			SystemInfo: metricsinfo.DeployMetrics{
				SystemVersion: os.Getenv(metricsinfo.GitCommitEnvKey),
				DeployMode:    os.Getenv(metricsinfo.DeployModeEnvKey),
			},
			// TODO(dragondriver): CreatedTime & UpdatedTime, easy but time-costing
			Type: typeutil.QueryNodeRole,
		},
		SystemConfigurations: metricsinfo.QueryNodeConfiguration{
			SearchReceiveBufSize:         Params.SearchReceiveBufSize,
			SearchPulsarBufSize:          Params.SearchPulsarBufSize,
			SearchResultReceiveBufSize:   Params.SearchResultReceiveBufSize,
			RetrieveReceiveBufSize:       Params.RetrieveReceiveBufSize,
			RetrievePulsarBufSize:        Params.RetrievePulsarBufSize,
			RetrieveResultReceiveBufSize: Params.RetrieveResultReceiveBufSize,

			SimdType: Params.SimdType,
		},
	}
	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, Params.QueryNodeID),
	}, nil
}

func checkSegmentMemory(segmentLoadInfos []*querypb.SegmentLoadInfo, historicalReplica, streamingReplica ReplicaInterface) error {
	historicalSegmentsMemSize := historicalReplica.getSegmentsMemSize()
	streamingSegmentsMemSize := streamingReplica.getSegmentsMemSize()
	usedRAMInMB := (historicalSegmentsMemSize + streamingSegmentsMemSize) / 1024.0 / 1024.0
	totalRAMInMB := Params.CacheSize * 1024.0

	segmentTotalSize := int64(0)
	for _, segInfo := range segmentLoadInfos {
		collectionID := segInfo.CollectionID
		segmentID := segInfo.SegmentID

		col, err := historicalReplica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}

		sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
		if err != nil {
			return err
		}

		segmentSize := int64(sizePerRecord) * segInfo.NumOfRows
		segmentTotalSize += segmentSize / 1024.0 / 1024.0
		// TODO: get threshold factor from param table
		thresholdMemSize := float64(totalRAMInMB) * 0.7

		log.Debug("memory stats when load segment",
			zap.Any("collectionIDs", collectionID),
			zap.Any("segmentID", segmentID),
			zap.Any("numOfRows", segInfo.NumOfRows),
			zap.Any("totalRAM(MB)", totalRAMInMB),
			zap.Any("usedRAM(MB)", usedRAMInMB),
			zap.Any("segmentTotalSize(MB)", segmentTotalSize),
			zap.Any("thresholdMemSize(MB)", thresholdMemSize),
		)
		if usedRAMInMB+segmentTotalSize > int64(thresholdMemSize) {
			return errors.New(fmt.Sprintln("load segment failed, OOM if load, "+
				"collectionID = ", collectionID, ", ",
				"usedRAM(MB) = ", usedRAMInMB, ", ",
				"segmentTotalSize(MB) = ", segmentTotalSize, ", ",
				"thresholdMemSize(MB) = ", thresholdMemSize))
		}
	}

	return nil
}
