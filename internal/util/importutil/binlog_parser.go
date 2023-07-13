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

package importutil

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

type BinlogParser struct {
	ctx                context.Context      // for canceling parse process
	collectionInfo     *CollectionInfo      // collection details including schema
	shardNum           int32                // sharding number of the collection
	blockSize          int64                // maximum size of a read block(unit:byte)
	chunkManager       storage.ChunkManager // storage interfaces to browse/read the files
	callFlushFunc      ImportFlushFunc      // call back function to flush segment
	updateProgressFunc func(percent int64)  // update working progress percent value

	// a timestamp to define the start time point of restore, data before this time point will be ignored
	// set this value to 0, all the data will be imported
	// set this value to math.MaxUint64, all the data will be ignored
	// the tsStartPoint value must be less/equal than tsEndPoint
	tsStartPoint uint64

	// a timestamp to define the end time point of restore, data after this time point will be ignored
	// set this value to 0, all the data will be ignored
	// set this value to math.MaxUint64, all the data will be imported
	// the tsEndPoint value must be larger/equal than tsStartPoint
	tsEndPoint uint64
}

func NewBinlogParser(ctx context.Context,
	collectionInfo *CollectionInfo,
	blockSize int64,
	chunkManager storage.ChunkManager,
	flushFunc ImportFlushFunc,
	updateProgressFunc func(percent int64),
	tsStartPoint uint64,
	tsEndPoint uint64) (*BinlogParser, error) {
	if collectionInfo == nil {
		log.Warn("Binlog parser: collection schema is nil")
		return nil, errors.New("collection schema is nil")
	}

	if chunkManager == nil {
		log.Warn("Binlog parser: chunk manager pointer is nil")
		return nil, errors.New("chunk manager pointer is nil")
	}

	if flushFunc == nil {
		log.Warn("Binlog parser: flush function is nil")
		return nil, errors.New("flush function is nil")
	}

	if tsStartPoint > tsEndPoint {
		log.Warn("Binlog parser: the tsStartPoint should be less than tsEndPoint",
			zap.Uint64("tsStartPoint", tsStartPoint), zap.Uint64("tsEndPoint", tsEndPoint))
		return nil, fmt.Errorf("Binlog parser: the tsStartPoint %d should be less than tsEndPoint %d", tsStartPoint, tsEndPoint)
	}

	v := &BinlogParser{
		ctx:                ctx,
		collectionInfo:     collectionInfo,
		blockSize:          blockSize,
		chunkManager:       chunkManager,
		callFlushFunc:      flushFunc,
		updateProgressFunc: updateProgressFunc,
		tsStartPoint:       tsStartPoint,
		tsEndPoint:         tsEndPoint,
	}

	return v, nil
}

// constructSegmentHolders builds a list of SegmentFilesHolder, each SegmentFilesHolder represents a segment folder
// For instance, the insertlogRoot is "backup/bak1/data/insert_log/435978159196147009/435978159196147010".
// 435978159196147009 is a collection id, 435978159196147010 is a partition id,
// there is a segment(id is 435978159261483009) under this partition.
// ListWithPrefix() will return all the insert logs under this partition:
//
//	"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/0/435978159903735811"
//	"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/1/435978159903735812"
//	"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/100/435978159903735809"
//	"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/101/435978159903735810"
//
// The deltalogRoot is "backup/bak1/data/delta_log/435978159196147009/435978159196147010".
// Then we get all the delta logs under this partition:
//
//	"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415105"
//
// In this function, we will constuct a list of SegmentFilesHolder objects, each SegmentFilesHolder holds the
// insert logs and delta logs of a segment.
func (p *BinlogParser) constructSegmentHolders(insertlogRoot string, deltalogRoot string) ([]*SegmentFilesHolder, error) {
	holders := make(map[int64]*SegmentFilesHolder)
	// TODO add context
	insertlogs, _, err := p.chunkManager.ListWithPrefix(context.TODO(), insertlogRoot, true)
	if err != nil {
		log.Warn("Binlog parser: list insert logs error", zap.Error(err))
		return nil, fmt.Errorf("failed to list insert logs with root path %s, error: %w", insertlogRoot, err)
	}

	// collect insert log paths
	log.Info("Binlog parser: list insert logs", zap.Int("logsCount", len(insertlogs)))
	for _, insertlog := range insertlogs {
		log.Info("Binlog parser: mapping insert log to segment", zap.String("insertlog", insertlog))
		filePath := path.Base(insertlog)
		// skip file with prefix '.', such as .success .DS_Store
		if strings.HasPrefix(filePath, ".") {
			log.Debug("file path might not be a real bin log", zap.String("filePath", filePath))
			continue
		}
		fieldPath := path.Dir(insertlog)
		fieldStrID := path.Base(fieldPath)
		fieldID, err := strconv.ParseInt(fieldStrID, 10, 64)
		if err != nil {
			log.Warn("Binlog parser: failed to parse field id", zap.String("fieldPath", fieldPath), zap.Error(err))
			return nil, fmt.Errorf("failed to parse field id from insert log path %s, error: %w", insertlog, err)
		}

		segmentPath := path.Dir(fieldPath)
		segmentStrID := path.Base(segmentPath)
		segmentID, err := strconv.ParseInt(segmentStrID, 10, 64)
		if err != nil {
			log.Warn("Binlog parser: failed to parse segment id", zap.String("segmentPath", segmentPath), zap.Error(err))
			return nil, fmt.Errorf("failed to parse segment id from insert log path %s, error: %w", insertlog, err)
		}

		holder, ok := holders[segmentID]
		if ok {
			holder.fieldFiles[fieldID] = append(holder.fieldFiles[fieldID], insertlog)
		} else {
			holder = &SegmentFilesHolder{
				segmentID:  segmentID,
				fieldFiles: make(map[int64][]string),
				deltaFiles: make([]string, 0),
			}
			holder.fieldFiles[fieldID] = make([]string, 0)
			holder.fieldFiles[fieldID] = append(holder.fieldFiles[fieldID], insertlog)
			holders[segmentID] = holder
		}
	}

	// sort the insert log paths of each field by ascendent sequence
	// there might be several insert logs under a field, for example:
	// 2 insert logs under field a: a_1, a_2
	// 2 insert logs under field b: b_1, b_2
	// the row count of a_1 is equal to b_1, the row count of a_2 is equal to b_2
	// when we read these logs, we firstly read a_1 and b_1, then read a_2 and b_2
	// so, here we must ensure the paths are arranged correctly
	segmentIDs := make([]int64, 0)
	for id, holder := range holders {
		segmentIDs = append(segmentIDs, id)
		for _, v := range holder.fieldFiles {
			sort.Strings(v)
		}
	}

	// collect delta log paths
	if len(deltalogRoot) > 0 {
		// TODO add context
		deltalogs, _, err := p.chunkManager.ListWithPrefix(context.TODO(), deltalogRoot, true)
		if err != nil {
			log.Warn("Binlog parser: failed to list delta logs", zap.Error(err))
			return nil, fmt.Errorf("failed to list delta logs, error: %w", err)
		}

		log.Info("Binlog parser: list delta logs", zap.Int("logsCount", len(deltalogs)))
		for _, deltalog := range deltalogs {
			log.Info("Binlog parser: mapping delta log to segment", zap.String("deltalog", deltalog))
			segmentPath := path.Dir(deltalog)
			segmentStrID := path.Base(segmentPath)
			segmentID, err := strconv.ParseInt(segmentStrID, 10, 64)
			if err != nil {
				log.Warn("Binlog parser: failed to parse segment id", zap.String("segmentPath", segmentPath), zap.Error(err))
				return nil, fmt.Errorf("failed to parse segment id from delta log path %s, error: %w", deltalog, err)
			}

			// if the segment id doesn't exist, no need to process this deltalog
			holder, ok := holders[segmentID]
			if ok {
				holder.deltaFiles = append(holder.deltaFiles, deltalog)
			}
		}
	}

	// since the map in golang is not sorted, we sort the segment id array to return holder list with ascending sequence
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })
	holdersList := make([]*SegmentFilesHolder, 0)
	for _, id := range segmentIDs {
		holdersList = append(holdersList, holders[id])
	}

	return holdersList, nil
}

func (p *BinlogParser) parseSegmentFiles(segmentHolder *SegmentFilesHolder) error {
	if segmentHolder == nil {
		log.Warn("Binlog parser: segment files holder is nil")
		return errors.New("segment files holder is nil")
	}

	adapter, err := NewBinlogAdapter(p.ctx, p.collectionInfo, p.blockSize,
		MaxTotalSizeInMemory, p.chunkManager, p.callFlushFunc, p.tsStartPoint, p.tsEndPoint)
	if err != nil {
		log.Warn("Binlog parser: failed to create binlog adapter", zap.Error(err))
		return fmt.Errorf("failed to create binlog adapter, error: %w", err)
	}

	return adapter.Read(segmentHolder)
}

// Parse requires two paths:
// 1. the insert log path of a partition
// 2. the delta log path of a partiion (optional)
func (p *BinlogParser) Parse(filePaths []string) error {
	if len(filePaths) != 1 && len(filePaths) != 2 {
		log.Warn("Binlog parser: illegal paths for binlog import, partition binlog path and delta path are required")
		return errors.New("illegal paths for binlog import, partition binlog path and delta path are required")
	}

	insertlogPath := filePaths[0]
	deltalogPath := ""
	if len(filePaths) == 2 {
		deltalogPath = filePaths[1]
	}
	log.Info("Binlog parser: target paths",
		zap.String("insertlogPath", insertlogPath),
		zap.String("deltalogPath", deltalogPath))

	segmentHolders, err := p.constructSegmentHolders(insertlogPath, deltalogPath)
	if err != nil {
		return err
	}

	updateProgress := func(readBatch int) {
		if p.updateProgressFunc != nil && len(segmentHolders) != 0 {
			percent := (readBatch * ProgressValueForPersist) / len(segmentHolders)
			log.Debug("Binlog parser: working progress", zap.Int("readBatch", readBatch),
				zap.Int("totalBatchCount", len(segmentHolders)), zap.Int("percent", percent))
			p.updateProgressFunc(int64(percent))
		}
	}

	for i, segmentHolder := range segmentHolders {
		err = p.parseSegmentFiles(segmentHolder)
		if err != nil {
			return err
		}
		updateProgress(i + 1)

		// trigger gb after each segment finished
		triggerGC()
	}

	return nil
}
