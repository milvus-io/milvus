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
package dataservice

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"go.uber.org/zap"
)

type statsHandler struct {
	meta *meta
}

func newStatsHandler(meta *meta) *statsHandler {
	return &statsHandler{
		meta: meta,
	}
}

func (handler *statsHandler) HandleSegmentStat(segStats *internalpb.SegmentStatisticsUpdates) error {
	segMeta, err := handler.meta.GetSegment(segStats.SegmentID)
	if err != nil {
		return err
	}

	if segStats.StartPosition != nil {
		segMeta.StartPosition = segStats.StartPosition
	}

	if segStats.EndPosition != nil {
		segMeta.EndPosition = segStats.EndPosition
	}

	segMeta.NumRows = segStats.NumRows
	log.Debug("stats_handler update segment", zap.Any("segmentID", segMeta.ID), zap.Any("State", segMeta.State))
	return handler.meta.UpdateSegmentStatistic(segMeta)
}
