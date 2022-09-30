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
	"errors"
	"path"
	"sort"
	"strconv"

	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"go.uber.org/zap"
)

type BinlogParser struct {
	collectionSchema *schemapb.CollectionSchema // collection schema
	shardNum         int32                      // sharding number of the collection
	segmentSize      int64                      // maximum size of a segment(unit:byte)
	chunkManager     storage.ChunkManager       // storage interfaces to browse/read the files
	callFlushFunc    ImportFlushFunc            // call back function to flush segment

	// a timestamp to define the end point of restore, data after this point will be ignored
	// set this value to 0, all the data will be ignored
	// set this value to math.MaxUint64, all the data will be imported
	tsEndPoint uint64
}

func NewBinlogParser(collectionSchema *schemapb.CollectionSchema,
	shardNum int32,
	segmentSize int64,
	chunkManager storage.ChunkManager,
	flushFunc ImportFlushFunc,
	tsEndPoint uint64) (*BinlogParser, error) {
	if collectionSchema == nil {
		log.Error("Binlog parser: collection schema is nil")
		return nil, errors.New("collection schema is nil")
	}

	if chunkManager == nil {
		log.Error("Binlog parser: chunk manager pointer is nil")
		return nil, errors.New("chunk manager pointer is nil")
	}

	if flushFunc == nil {
		log.Error("Binlog parser: flush function is nil")
		return nil, errors.New("flush function is nil")
	}

	v := &BinlogParser{
		collectionSchema: collectionSchema,
		shardNum:         shardNum,
		segmentSize:      segmentSize,
		chunkManager:     chunkManager,
		callFlushFunc:    flushFunc,
		tsEndPoint:       tsEndPoint,
	}

	return v, nil
}

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
		log.Error("Binlog parser: list insert logs error", zap.Error(err))
		return nil, err
	}

	// collect insert log paths
	log.Info("Binlog parser: list insert logs", zap.Int("logsCount", len(insertlogs)))
	for _, insertlog := range insertlogs {
		log.Info("Binlog parser: mapping insert log to segment", zap.String("insertlog", insertlog))
		fieldPath := path.Dir(insertlog)
		fieldStrID := path.Base(fieldPath)
		fieldID, err := strconv.ParseInt(fieldStrID, 10, 64)
		if err != nil {
			log.Error("Binlog parser: parse field id error", zap.String("fieldPath", fieldPath), zap.Error(err))
			return nil, err
		}

		segmentPath := path.Dir(fieldPath)
		segmentStrID := path.Base(segmentPath)
		segmentID, err := strconv.ParseInt(segmentStrID, 10, 64)
		if err != nil {
			log.Error("Binlog parser: parse segment id error", zap.String("segmentPath", segmentPath), zap.Error(err))
			return nil, err
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
	for _, holder := range holders {
		for _, v := range holder.fieldFiles {
			sort.Strings(v)
		}
	}

	// collect delta log paths
	if len(deltalogRoot) > 0 {
		// TODO add context
		deltalogs, _, err := p.chunkManager.ListWithPrefix(context.TODO(), deltalogRoot, true)
		if err != nil {
			log.Error("Binlog parser: list delta logs error", zap.Error(err))
			return nil, err
		}

		log.Info("Binlog parser: list delta logs", zap.Int("logsCount", len(deltalogs)))
		for _, deltalog := range deltalogs {
			log.Info("Binlog parser: mapping delta log to segment", zap.String("deltalog", deltalog))
			segmentPath := path.Dir(deltalog)
			segmentStrID := path.Base(segmentPath)
			segmentID, err := strconv.ParseInt(segmentStrID, 10, 64)
			if err != nil {
				log.Error("Binlog parser: parse segment id error", zap.String("segmentPath", segmentPath), zap.Error(err))
				return nil, err
			}

			// if the segment id doesn't exist, no need to process this deltalog
			holder, ok := holders[segmentID]
			if ok {
				holder.deltaFiles = append(holder.deltaFiles, deltalog)
			}
		}
	}

	holdersList := make([]*SegmentFilesHolder, 0)
	for _, holder := range holders {
		holdersList = append(holdersList, holder)
	}

	return holdersList, nil
}

func (p *BinlogParser) parseSegmentFiles(segmentHolder *SegmentFilesHolder) error {
	if segmentHolder == nil {
		log.Error("Binlog parser: segment files holder is nil")
		return errors.New("segment files holder is nil")
	}

	adapter, err := NewBinlogAdapter(p.collectionSchema, p.shardNum, p.segmentSize,
		MaxTotalSizeInMemory, p.chunkManager, p.callFlushFunc, p.tsEndPoint)
	if err != nil {
		log.Error("Binlog parser: failed to create binlog adapter", zap.Error(err))
		return err
	}

	return adapter.Read(segmentHolder)
}

// This functions requires two paths:
// 1. the insert log path of a partition
// 2. the delta log path of a partiion (optional)
func (p *BinlogParser) Parse(filePaths []string) error {
	if len(filePaths) != 1 && len(filePaths) != 2 {
		log.Error("Binlog parser: illegal paths for binlog import")
		return errors.New("illegal paths for binlog import, partition binlog path and partition delta path are required")
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

	for _, segmentHolder := range segmentHolders {
		err = p.parseSegmentFiles(segmentHolder)
		if err != nil {
			return err
		}

		// trigger gb after each segment finished
		triggerGC()
	}

	return nil
}
