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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

// A struct to hold insert log paths and delta log paths of a segment
type SegmentFilesHolder struct {
	segmentID  int64                        // id of the segment
	fieldFiles map[storage.FieldID][]string // mapping of field id and data file path
	deltaFiles []string                     // a list of delta log file path, typically has only one item
}

// Adapter class to process insertlog/deltalog of a backuped segment
// This class do the following works:
// 1. read insert log of each field, then constructs SegmentData in memory.
// 2. read delta log to remove deleted entities(TimeStampField is used to apply or skip the operation).
// 3. split data according to shard number
// 4. call the callFlushFunc function to flush data into new binlog file if data size reaches blockSize.
type BinlogAdapter struct {
	ctx            context.Context      // for canceling parse process
	collectionInfo *CollectionInfo      // collection details including schema
	chunkManager   storage.ChunkManager // storage interfaces to read binlog files
	callFlushFunc  ImportFlushFunc      // call back function to flush segment
	blockSize      int64                // maximum size of a read block(unit:byte)
	maxTotalSize   int64                // maximum size of in-memory segments(unit:byte)

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

func NewBinlogAdapter(ctx context.Context,
	collectionInfo *CollectionInfo,
	blockSize int64,
	maxTotalSize int64,
	chunkManager storage.ChunkManager,
	flushFunc ImportFlushFunc,
	tsStartPoint uint64,
	tsEndPoint uint64) (*BinlogAdapter, error) {
	if collectionInfo == nil {
		log.Warn("Binlog adapter: collection schema is nil")
		return nil, errors.New("collection schema is nil")
	}

	// binlog import doesn't support partition key, the caller must specify one partition for importing
	if len(collectionInfo.PartitionIDs) != 1 {
		log.Warn("Binlog adapter: target partition must be only one", zap.Int("partitions", len(collectionInfo.PartitionIDs)))
		return nil, errors.New("target partition must be only one")
	}

	if chunkManager == nil {
		log.Warn("Binlog adapter: chunk manager pointer is nil")
		return nil, errors.New("chunk manager pointer is nil")
	}

	if flushFunc == nil {
		log.Warn("Binlog adapter: flush function is nil")
		return nil, errors.New("flush function is nil")
	}

	adapter := &BinlogAdapter{
		ctx:            ctx,
		collectionInfo: collectionInfo,
		chunkManager:   chunkManager,
		callFlushFunc:  flushFunc,
		blockSize:      blockSize,
		maxTotalSize:   maxTotalSize,
		tsStartPoint:   tsStartPoint,
		tsEndPoint:     tsEndPoint,
	}

	// amend the segment size to avoid portential OOM risk
	if adapter.blockSize > MaxSegmentSizeInMemory {
		adapter.blockSize = MaxSegmentSizeInMemory
	}

	return adapter, nil
}

func (p *BinlogAdapter) Read(segmentHolder *SegmentFilesHolder) error {
	if segmentHolder == nil {
		log.Warn("Binlog adapter: segment files holder is nil")
		return errors.New("segment files holder is nil")
	}

	log.Info("Binlog adapter: read segment", zap.Int64("segmentID", segmentHolder.segmentID))

	// step 1: verify the file count by collection schema
	err := p.verify(segmentHolder)
	if err != nil {
		return err
	}

	// step 2: read the delta log to prepare delete list, and combine lists into one dict
	intDeletedList, strDeletedList, err := p.readDeltalogs(segmentHolder)
	if err != nil {
		return err
	}

	// step 3: read binlog files batch by batch
	// Assume the collection has 2 fields: a and b
	// a has these binlog files: a_1, a_2, a_3 ...
	// b has these binlog files: b_1, b_2, b_3 ...
	// Then first round read a_1 and b_1, second round read a_2 and b_2, etc...
	// deleted list will be used to remove deleted entities
	// if accumulate data exceed blockSize, call callFlushFunc to generate new binlog file
	batchCount := 0
	for _, files := range segmentHolder.fieldFiles {
		batchCount = len(files)
		break
	}

	// prepare shards in-memory data
	shardsData := make([]ShardData, 0, p.collectionInfo.ShardNum)
	for i := 0; i < int(p.collectionInfo.ShardNum); i++ {
		shardData := initShardData(p.collectionInfo.Schema, p.collectionInfo.PartitionIDs)
		if shardData == nil {
			log.Warn("Binlog adapter: fail to initialize in-memory segment data", zap.Int("shardID", i))
			return fmt.Errorf("fail to initialize in-memory segment data for shard id %d", i)
		}
		shardsData = append(shardsData, shardData)
	}

	// read binlog files batch by batch
	primaryKey := p.collectionInfo.PrimaryKey
	for i := 0; i < batchCount; i++ {
		// batchFiles excludes the primary key field and the timestamp field
		// timestamp field is used to compare the tsEndPoint to skip some rows, no need to pass old timestamp to new segment.
		// once a new segment generated, the timestamp field will be re-generated, too.
		batchFiles := make(map[storage.FieldID]string)
		for fieldID, files := range segmentHolder.fieldFiles {
			if fieldID == primaryKey.GetFieldID() || fieldID == common.TimeStampField {
				continue
			}
			batchFiles[fieldID] = files[i]
		}
		log.Info("Binlog adapter: batch files to read", zap.Any("batchFiles", batchFiles))

		// read primary keys firstly
		primaryLog := segmentHolder.fieldFiles[primaryKey.GetFieldID()][i] // no need to check existence, already verified
		log.Info("Binlog adapter: prepare to read primary key binglog",
			zap.Int64("pk", primaryKey.GetFieldID()), zap.String("logPath", primaryLog))
		intList, strList, err := p.readPrimaryKeys(primaryLog)
		if err != nil {
			return err
		}

		// read timestamps list
		timestampLog := segmentHolder.fieldFiles[common.TimeStampField][i] // no need to check existence, already verified
		log.Info("Binlog adapter: prepare to read timestamp binglog", zap.Any("logPath", timestampLog))
		timestampList, err := p.readTimestamp(timestampLog)
		if err != nil {
			return err
		}

		var shardList []int32
		if primaryKey.GetDataType() == schemapb.DataType_Int64 {
			// calculate a shard num list by primary keys and deleted entities
			shardList, err = p.getShardingListByPrimaryInt64(intList, timestampList, shardsData, intDeletedList)
			if err != nil {
				return err
			}
		} else if primaryKey.GetDataType() == schemapb.DataType_VarChar {
			// calculate a shard num list by primary keys and deleted entities
			shardList, err = p.getShardingListByPrimaryVarchar(strList, timestampList, shardsData, strDeletedList)
			if err != nil {
				return err
			}
		} else {
			log.Warn("Binlog adapter: unsupported primary key type", zap.Int("type", int(primaryKey.GetDataType())))
			return fmt.Errorf("unsupported primary key type %d, primary key should be int64 or varchar", primaryKey.GetDataType())
		}

		// if shardList is empty, that means all the primary keys have been deleted(or skipped), no need to read other files
		if len(shardList) == 0 {
			continue
		}

		// read other insert logs and use the shardList to do sharding
		for fieldID, file := range batchFiles {
			// outside context might be canceled(service stop, or future enhancement for canceling import task)
			if isCanceled(p.ctx) {
				log.Warn("Binlog adapter: import task was canceled")
				return errors.New("import task was canceled")
			}

			err = p.readInsertlog(fieldID, file, shardsData, shardList)
			if err != nil {
				return err
			}
		}

		// flush segment whose size exceed blockSize
		err = tryFlushBlocks(p.ctx, shardsData, p.collectionInfo.Schema, p.callFlushFunc, p.blockSize, p.maxTotalSize, false)
		if err != nil {
			return err
		}
	}

	// finally, force to flush
	return tryFlushBlocks(p.ctx, shardsData, p.collectionInfo.Schema, p.callFlushFunc, p.blockSize, p.maxTotalSize, true)
}

// verify method verify the schema and binlog files
//  1. each field must has binlog file
//  2. binlog file count of each field must be equal
//  3. the collectionSchema doesn't contain TimeStampField and RowIDField since the import_wrapper excludes them,
//     but the segmentHolder.fieldFiles need to contains the two fields.
func (p *BinlogAdapter) verify(segmentHolder *SegmentFilesHolder) error {
	if segmentHolder == nil {
		log.Warn("Binlog adapter: segment files holder is nil")
		return errors.New("segment files holder is nil")
	}

	firstFieldFileCount := 0
	//  each field must has binlog file
	for i := 0; i < len(p.collectionInfo.Schema.Fields); i++ {
		schema := p.collectionInfo.Schema.Fields[i]

		files, ok := segmentHolder.fieldFiles[schema.FieldID]
		if !ok {
			log.Warn("Binlog adapter: a field has no binlog file", zap.Int64("fieldID", schema.FieldID))
			return fmt.Errorf("the field %d has no binlog file", schema.FieldID)
		}

		if i == 0 {
			firstFieldFileCount = len(files)
		}
	}

	// the segmentHolder.fieldFiles need to contains RowIDField
	_, ok := segmentHolder.fieldFiles[common.RowIDField]
	if !ok {
		log.Warn("Binlog adapter: the binlog files of RowIDField is missed")
		return errors.New("the binlog files of RowIDField is missed")
	}

	// the segmentHolder.fieldFiles need to contains TimeStampField
	_, ok = segmentHolder.fieldFiles[common.TimeStampField]
	if !ok {
		log.Warn("Binlog adapter: the binlog files of TimeStampField is missed")
		return errors.New("the binlog files of TimeStampField is missed")
	}

	// binlog file count of each field must be equal
	for _, files := range segmentHolder.fieldFiles {
		if firstFieldFileCount != len(files) {
			log.Warn("Binlog adapter: file count of each field must be equal", zap.Int("firstFieldFileCount", firstFieldFileCount))
			return fmt.Errorf("binlog file count of each field must be equal, first field files count: %d, other field files count: %d",
				firstFieldFileCount, len(files))
		}
	}

	return nil
}

// readDeltalogs method reads data from deltalog, and convert to a dict
// The deltalog data is a list, to improve performance of next step, we convert it to a dict,
// key is the deleted ID, value is operation timestamp which is used to apply or skip the delete operation.
func (p *BinlogAdapter) readDeltalogs(segmentHolder *SegmentFilesHolder) (map[int64]uint64, map[string]uint64, error) {
	deleteLogs, err := p.decodeDeleteLogs(segmentHolder)
	if err != nil {
		return nil, nil, err
	}

	if len(deleteLogs) == 0 {
		log.Info("Binlog adapter: no deletion for segment", zap.Int64("segmentID", segmentHolder.segmentID))
		return nil, nil, nil // no deletion
	}

	primaryKey := p.collectionInfo.PrimaryKey
	if primaryKey.GetDataType() == schemapb.DataType_Int64 {
		deletedIDDict := make(map[int64]uint64)
		for _, deleteLog := range deleteLogs {
			deletedIDDict[deleteLog.Pk.GetValue().(int64)] = deleteLog.Ts
		}
		log.Info("Binlog adapter: count of deleted entities", zap.Int("deletedCount", len(deletedIDDict)))
		return deletedIDDict, nil, nil
	} else if primaryKey.GetDataType() == schemapb.DataType_VarChar {
		deletedIDDict := make(map[string]uint64)
		for _, deleteLog := range deleteLogs {
			deletedIDDict[deleteLog.Pk.GetValue().(string)] = deleteLog.Ts
		}
		log.Info("Binlog adapter: count of deleted entities", zap.Int("deletedCount", len(deletedIDDict)))
		return nil, deletedIDDict, nil
	} else {
		log.Warn("Binlog adapter: unsupported primary key type", zap.Int("type", int(primaryKey.GetDataType())))
		return nil, nil, fmt.Errorf("unsupported primary key type %d, primary key should be int64 or varchar", primaryKey.GetDataType())
	}
}

// decodeDeleteLogs decodes string array(read from delta log) to storage.DeleteLog array
func (p *BinlogAdapter) decodeDeleteLogs(segmentHolder *SegmentFilesHolder) ([]*storage.DeleteLog, error) {
	// step 1: read all delta logs to construct a string array, each string is marshaled from storage.DeleteLog
	stringArray := make([]string, 0)
	for _, deltalog := range segmentHolder.deltaFiles {
		deltaStrings, err := p.readDeltalog(deltalog)
		if err != nil {
			return nil, err
		}
		stringArray = append(stringArray, deltaStrings...)
	}

	if len(stringArray) == 0 {
		return nil, nil // no delete log, return directly
	}

	// print out the first deletion information for diagnose purpose
	log.Info("Binlog adapter: total deletion count", zap.Int("count", len(stringArray)), zap.String("firstDeletion", stringArray[0]))

	// step 2: decode each string to a storage.DeleteLog object
	deleteLogs := make([]*storage.DeleteLog, 0)
	for i := 0; i < len(stringArray); i++ {
		deleteLog, err := p.decodeDeleteLog(stringArray[i])
		if err != nil {
			return nil, err
		}

		// only the ts between tsStartPoint and tsEndPoint is effective
		// ignore deletions whose timestamp is larger than the tsEndPoint or less than tsStartPoint
		if deleteLog.Ts >= p.tsStartPoint && deleteLog.Ts <= p.tsEndPoint {
			deleteLogs = append(deleteLogs, deleteLog)
		}
	}
	log.Info("Binlog adapter: deletion count after filtering", zap.Int("count", len(deleteLogs)))

	// step 3: verify the current collection primary key type and the delete logs data type
	primaryKey := p.collectionInfo.PrimaryKey
	for i := 0; i < len(deleteLogs); i++ {
		if deleteLogs[i].PkType != int64(primaryKey.GetDataType()) {
			log.Warn("Binlog adapter: delta log data type is not equal to collection's primary key data type",
				zap.Int64("deltaDataType", deleteLogs[i].PkType),
				zap.Int64("pkDataType", int64(primaryKey.GetDataType())))
			return nil, fmt.Errorf("delta log data type %d is not equal to collection's primary key data type %d",
				deleteLogs[i].PkType, primaryKey.GetDataType())
		}
	}

	return deleteLogs, nil
}

// decodeDeleteLog decodes a string to storage.DeleteLog
// Note: the following code is mainly come from data_codec.go, I suppose the code can compatible with old version 2.0
func (p *BinlogAdapter) decodeDeleteLog(deltaStr string) (*storage.DeleteLog, error) {
	deleteLog := &storage.DeleteLog{}
	if err := json.Unmarshal([]byte(deltaStr), deleteLog); err != nil {
		// compatible with versions that only support int64 type primary keys
		// compatible with fmt.Sprintf("%d,%d", pk, ts)
		// compatible error info (unmarshal err invalid character ',' after top-level value)
		splits := strings.Split(deltaStr, ",")
		if len(splits) != 2 {
			log.Warn("Binlog adapter: the format of deletion string is incorrect", zap.String("deltaStr", deltaStr))
			return nil, fmt.Errorf("the format of deletion string is incorrect, '%s' can not be split", deltaStr)
		}
		pk, err := strconv.ParseInt(splits[0], 10, 64)
		if err != nil {
			log.Warn("Binlog adapter: failed to parse primary key of deletion string from old version",
				zap.String("deltaStr", deltaStr), zap.Error(err))
			return nil, fmt.Errorf("failed to parse primary key of deletion string '%s' from old version, error: %w", deltaStr, err)
		}
		deleteLog.Pk = &storage.Int64PrimaryKey{
			Value: pk,
		}
		deleteLog.PkType = int64(schemapb.DataType_Int64)
		deleteLog.Ts, err = strconv.ParseUint(splits[1], 10, 64)
		if err != nil {
			log.Warn("Binlog adapter: failed to parse timestamp of deletion string from old version",
				zap.String("deltaStr", deltaStr), zap.Error(err))
			return nil, fmt.Errorf("failed to parse timestamp of deletion string '%s' from old version, error: %w", deltaStr, err)
		}
	}

	return deleteLog, nil
}

// readDeltalog parses a delta log file. Each delta log data type is varchar, marshaled from an array of storage.DeleteLog objects.
func (p *BinlogAdapter) readDeltalog(logPath string) ([]string, error) {
	// open the delta log file
	binlogFile, err := NewBinlogFile(p.chunkManager)
	if err != nil {
		log.Warn("Binlog adapter: failed to initialize binlog file", zap.String("logPath", logPath), zap.Error(err))
		return nil, fmt.Errorf("failed to initialize binlog file '%s', error: %w", logPath, err)
	}

	err = binlogFile.Open(logPath)
	if err != nil {
		log.Warn("Binlog adapter: failed to open delta log", zap.String("logPath", logPath), zap.Error(err))
		return nil, fmt.Errorf("failed to open delta log '%s', error: %w", logPath, err)
	}
	defer binlogFile.Close()

	// delta log type is varchar, return a string array(marshaled from an array of storage.DeleteLog objects)
	data, err := binlogFile.ReadVarchar()
	if err != nil {
		log.Warn("Binlog adapter: failed to read delta log", zap.String("logPath", logPath), zap.Error(err))
		return nil, fmt.Errorf("failed to read delta log '%s', error: %w", logPath, err)
	}
	log.Info("Binlog adapter: successfully read deltalog", zap.Int("deleteCount", len(data)))

	return data, nil
}

// readTimestamp method reads data from int64 field, currently we use it to read the timestamp field.
func (p *BinlogAdapter) readTimestamp(logPath string) ([]int64, error) {
	// open the log file
	binlogFile, err := NewBinlogFile(p.chunkManager)
	if err != nil {
		log.Warn("Binlog adapter: failed to initialize binlog file", zap.String("logPath", logPath), zap.Error(err))
		return nil, fmt.Errorf("failed to initialize binlog file '%s', error: %w", logPath, err)
	}

	err = binlogFile.Open(logPath)
	if err != nil {
		log.Warn("Binlog adapter: failed to open timestamp log file", zap.String("logPath", logPath))
		return nil, fmt.Errorf("failed to open timestamp log file '%s', error: %w", logPath, err)
	}
	defer binlogFile.Close()

	// read int64 data
	int64List, err := binlogFile.ReadInt64()
	if err != nil {
		log.Warn("Binlog adapter: failed to read timestamp data from log file", zap.String("logPath", logPath))
		return nil, fmt.Errorf("failed to read timestamp data from log file '%s', error: %w", logPath, err)
	}

	log.Info("Binlog adapter: read timestamp from log file", zap.Int("tsCount", len(int64List)))

	return int64List, nil
}

// readPrimaryKeys method reads primary keys from insert log.
func (p *BinlogAdapter) readPrimaryKeys(logPath string) ([]int64, []string, error) {
	// open the delta log file
	binlogFile, err := NewBinlogFile(p.chunkManager)
	if err != nil {
		log.Warn("Binlog adapter: failed to initialize binlog file", zap.String("logPath", logPath), zap.Error(err))
		return nil, nil, fmt.Errorf("failed to initialize binlog file '%s', error: %w", logPath, err)
	}

	err = binlogFile.Open(logPath)
	if err != nil {
		log.Warn("Binlog adapter: failed to open primary key binlog", zap.String("logPath", logPath))
		return nil, nil, fmt.Errorf("failed to open primary key binlog '%s', error: %w", logPath, err)
	}
	defer binlogFile.Close()

	// primary key can be int64 or varchar, we need to handle the two cases
	primaryKey := p.collectionInfo.PrimaryKey
	if primaryKey.GetDataType() == schemapb.DataType_Int64 {
		idList, err := binlogFile.ReadInt64()
		if err != nil {
			log.Warn("Binlog adapter: failed to read int64 primary key from binlog", zap.String("logPath", logPath), zap.Error(err))
			return nil, nil, fmt.Errorf("failed to read int64 primary key from binlog '%s', error: %w", logPath, err)
		}
		log.Info("Binlog adapter: succeed to read int64 primary key binlog", zap.Int("len", len(idList)))
		return idList, nil, nil
	} else if primaryKey.GetDataType() == schemapb.DataType_VarChar {
		idList, err := binlogFile.ReadVarchar()
		if err != nil {
			log.Warn("Binlog adapter: failed to read varchar primary key from binlog", zap.String("logPath", logPath), zap.Error(err))
			return nil, nil, fmt.Errorf("failed to read varchar primary key from binlog '%s', error: %w", logPath, err)
		}
		log.Info("Binlog adapter: succeed to read varchar primary key binlog", zap.Int("len", len(idList)))
		return nil, idList, nil
	} else {
		log.Warn("Binlog adapter: unsupported primary key type", zap.Int("type", int(primaryKey.GetDataType())))
		return nil, nil, fmt.Errorf("unsupported primary key type %d, primary key should be int64 or varchar", primaryKey.GetDataType())
	}
}

// getShardingListByPrimaryInt64 method generates a shard id list by primary key(int64) list and deleted list.
// For example, an insert log has 10 rows, the no.3 and no.7 has been deleted, shardNum=2, the shardList could be:
// [0, 1, -1, 1, 0, 1, -1, 1, 0, 1]
// Compare timestampList with tsEndPoint to skip some rows.
func (p *BinlogAdapter) getShardingListByPrimaryInt64(primaryKeys []int64,
	timestampList []int64,
	memoryData []ShardData,
	intDeletedList map[int64]uint64) ([]int32, error) {
	if len(timestampList) != len(primaryKeys) {
		log.Warn("Binlog adapter: primary key length is not equal to timestamp list length",
			zap.Int("primaryKeysLen", len(primaryKeys)), zap.Int("timestampLen", len(timestampList)))
		return nil, fmt.Errorf("primary key length %d is not equal to timestamp list length %d", len(primaryKeys), len(timestampList))
	}

	log.Info("Binlog adapter: building shard list", zap.Int("pkLen", len(primaryKeys)), zap.Int("tsLen", len(timestampList)))

	actualDeleted := 0
	excluded := 0
	shardList := make([]int32, 0, len(primaryKeys))
	primaryKey := p.collectionInfo.PrimaryKey
	for i, key := range primaryKeys {
		// if this entity's timestamp is greater than the tsEndPoint, or less than tsStartPoint, set shardID = -1 to skip this entity
		// timestamp is stored as int64 type in log file, actually it is uint64, compare with uint64
		ts := timestampList[i]
		if uint64(ts) > p.tsEndPoint || uint64(ts) < p.tsStartPoint {
			shardList = append(shardList, -1)
			excluded++
			continue
		}

		_, deleted := intDeletedList[key]
		// if the key exists in intDeletedList, that means this entity has been deleted
		if deleted {
			shardList = append(shardList, -1) // this entity has been deleted, set shardID = -1 and skip this entity
			actualDeleted++
		} else {
			hash, _ := typeutil.Hash32Int64(key)
			shardID := hash % uint32(p.collectionInfo.ShardNum)
			partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
			fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
			field := fields[primaryKey.GetFieldID()]               // initBlockData() can ensure the existence, no need to check here

			// append the entity to primary key's FieldData
			field.(*storage.Int64FieldData).Data = append(field.(*storage.Int64FieldData).Data, key)

			shardList = append(shardList, int32(shardID))
		}
	}
	log.Info("Binlog adapter: succeed to calculate a shard list", zap.Int("actualDeleted", actualDeleted),
		zap.Int("excluded", excluded), zap.Int("len", len(shardList)))

	return shardList, nil
}

// getShardingListByPrimaryVarchar method generates a shard id list by primary key(varchar) list and deleted list.
// For example, an insert log has 10 rows, the no.3 and no.7 has been deleted, shardNum=2, the shardList could be:
// [0, 1, -1, 1, 0, 1, -1, 1, 0, 1]
func (p *BinlogAdapter) getShardingListByPrimaryVarchar(primaryKeys []string,
	timestampList []int64,
	memoryData []ShardData,
	strDeletedList map[string]uint64) ([]int32, error) {
	if len(timestampList) != len(primaryKeys) {
		log.Warn("Binlog adapter: primary key length is not equal to timestamp list length",
			zap.Int("primaryKeysLen", len(primaryKeys)), zap.Int("timestampLen", len(timestampList)))
		return nil, fmt.Errorf("primary key length %d is not equal to timestamp list length %d", len(primaryKeys), len(timestampList))
	}

	log.Info("Binlog adapter: building shard list", zap.Int("pkLen", len(primaryKeys)), zap.Int("tsLen", len(timestampList)))

	actualDeleted := 0
	excluded := 0
	shardList := make([]int32, 0, len(primaryKeys))
	primaryKey := p.collectionInfo.PrimaryKey
	for i, key := range primaryKeys {
		// if this entity's timestamp is greater than the tsEndPoint, or less than tsStartPoint, set shardID = -1 to skip this entity
		// timestamp is stored as int64 type in log file, actually it is uint64, compare with uint64
		ts := timestampList[i]
		if uint64(ts) > p.tsEndPoint || uint64(ts) < p.tsStartPoint {
			shardList = append(shardList, -1)
			excluded++
			continue
		}

		_, deleted := strDeletedList[key]
		// if exists in strDeletedList, that means this entity has been deleted
		if deleted {
			shardList = append(shardList, -1) // this entity has been deleted, set shardID = -1 and skip this entity
			actualDeleted++
		} else {
			hash := typeutil.HashString2Uint32(key)
			shardID := hash % uint32(p.collectionInfo.ShardNum)
			partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
			fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
			field := fields[primaryKey.GetFieldID()]               // initBlockData() can ensure the existence, no need to check existence here

			// append the entity to primary key's FieldData
			field.(*storage.StringFieldData).Data = append(field.(*storage.StringFieldData).Data, key)

			shardList = append(shardList, int32(shardID))
		}
	}
	log.Info("Binlog adapter: succeed to calculate a shard list", zap.Int("actualDeleted", actualDeleted),
		zap.Int("excluded", excluded), zap.Int("len", len(shardList)))

	return shardList, nil
}

// Sometimes the fieldID doesn't exist in the memoryData in the following case:
// Use an old backup tool(v0.2.2) to backup a collection of milvus v2.2.9, use a new backup tool to restore the collection
func (p *BinlogAdapter) verifyField(fieldID storage.FieldID, memoryData []ShardData) error {
	for _, partitions := range memoryData {
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		_, ok := fields[fieldID]
		if !ok {
			log.Warn("Binlog adapter: the field ID doesn't exist in collection schema", zap.Int64("fieldID", fieldID))
			return fmt.Errorf("the field ID %d doesn't exist in collection schema", fieldID)
		}
	}
	return nil
}

// readInsertlog method reads an insert log, and split the data into different shards according to a shard list
// The shardList is a list to tell which row belong to which shard, returned by getShardingListByPrimaryXXX()
// For deleted rows, we say its shard id is -1.
// For example, an insert log has 10 rows, the no.3 and no.7 has been deleted, shardNum=2, the shardList could be:
// [0, 1, -1, 1, 0, 1, -1, 1, 0, 1]
// This method put each row into different FieldData according to its shard id and field id,
// so, the no.1, no.5, no.9 will be put into shard_0
// the no.2, no.4, no.6, no.8, no.10 will be put into shard_1
// Note: the row count of insert log need to be equal to length of shardList
func (p *BinlogAdapter) readInsertlog(fieldID storage.FieldID, logPath string,
	memoryData []ShardData, shardList []int32) error {
	err := p.verifyField(fieldID, memoryData)
	if err != nil {
		log.Warn("Binlog adapter: could not read binlog file", zap.String("logPath", logPath), zap.Error(err))
		return fmt.Errorf("could not read binlog file %s, error: %w", logPath, err)
	}

	// open the insert log file
	binlogFile, err := NewBinlogFile(p.chunkManager)
	if err != nil {
		log.Warn("Binlog adapter: failed to initialize binlog file", zap.String("logPath", logPath), zap.Error(err))
		return fmt.Errorf("failed to initialize binlog file %s, error: %w", logPath, err)
	}

	err = binlogFile.Open(logPath)
	if err != nil {
		log.Warn("Binlog adapter: failed to open insert log", zap.String("logPath", logPath), zap.Error(err))
		return fmt.Errorf("failed to open insert log %s, error: %w", logPath, err)
	}
	defer binlogFile.Close()

	// read data according to data type
	switch binlogFile.DataType() {
	case schemapb.DataType_Bool:
		data, err := binlogFile.ReadBool()
		if err != nil {
			return err
		}

		err = p.dispatchBoolToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int8:
		data, err := binlogFile.ReadInt8()
		if err != nil {
			return err
		}

		err = p.dispatchInt8ToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int16:
		data, err := binlogFile.ReadInt16()
		if err != nil {
			return err
		}

		err = p.dispatchInt16ToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int32:
		data, err := binlogFile.ReadInt32()
		if err != nil {
			return err
		}

		err = p.dispatchInt32ToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Int64:
		data, err := binlogFile.ReadInt64()
		if err != nil {
			return err
		}

		err = p.dispatchInt64ToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Float:
		data, err := binlogFile.ReadFloat()
		if err != nil {
			return err
		}

		err = p.dispatchFloatToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_Double:
		data, err := binlogFile.ReadDouble()
		if err != nil {
			return err
		}

		err = p.dispatchDoubleToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		data, err := binlogFile.ReadVarchar()
		if err != nil {
			return err
		}

		err = p.dispatchVarcharToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_JSON:
		data, err := binlogFile.ReadJSON()
		if err != nil {
			return err
		}

		err = p.dispatchBytesToShards(data, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_BinaryVector:
		data, dim, err := binlogFile.ReadBinaryVector()
		if err != nil {
			return err
		}

		err = p.dispatchBinaryVecToShards(data, dim, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	case schemapb.DataType_FloatVector:
		data, dim, err := binlogFile.ReadFloatVector()
		if err != nil {
			return err
		}

		err = p.dispatchFloatVecToShards(data, dim, memoryData, shardList, fieldID)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported data type %d", binlogFile.DataType())
	}
	log.Info("Binlog adapter: read data into shard list", zap.Int("dataType", int(binlogFile.DataType())), zap.Int("shardLen", len(shardList)))

	return nil
}

func (p *BinlogAdapter) dispatchBoolToShards(data []bool, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: bool field row count is not equal to shard list row count %d", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("bool field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: bool field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("bool field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.BoolFieldData)        // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is bool type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is bool type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchInt8ToShards(data []int8, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: int8 field row count is not equal to shard list row count", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("int8 field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entity acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: int8 field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("int8 field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.Int8FieldData)        // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is int8 type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is int8 type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchInt16ToShards(data []int16, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: int16 field row count is not equal to shard list row count", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("int16 field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: int16 field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("int16 field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.Int16FieldData)       // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is int16 type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is int16 type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchInt32ToShards(data []int32, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: int32 field row count is not equal to shard list row count", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("int32 field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: int32 field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("int32 field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.Int32FieldData)       // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is int32 type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is int32 type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchInt64ToShards(data []int64, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: int64 field row count is not equal to shard list row count", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("int64 field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: int64 field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("int64 field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.Int64FieldData)       // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is int64 type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is int64 type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchFloatToShards(data []float32, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: float field row count is not equal to shard list row count", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("float field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: float field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("float field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.FloatFieldData)       // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is float type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is float type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchDoubleToShards(data []float64, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: double field row count is not equal to shard list row count", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("double field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: double field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("double field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.DoubleFieldData)      // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is double type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is double type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchVarcharToShards(data []string, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: varchar field row count is not equal to shard list row count", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("varchar field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: varchar field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("varchar field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.StringFieldData)      // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is varchar type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is varchar type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchBytesToShards(data [][]byte, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	if len(data) != len(shardList) {
		log.Warn("Binlog adapter: JSON field row count is not equal to shard list row count", zap.Int("dataLen", len(data)), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("varchar JSON row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i, val := range data {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: JSON field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("JSON field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.JSONFieldData)        // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is JSON type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is JSON type, unequal to field %d", fieldID)
		}
		fieldData.Data = append(fieldData.Data, val)
	}

	return nil
}

func (p *BinlogAdapter) dispatchBinaryVecToShards(data []byte, dim int, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	bytesPerVector := dim / 8
	count := len(data) / bytesPerVector
	if count != len(shardList) {
		log.Warn("Binlog adapter: binary vector field row count is not equal to shard list row count",
			zap.Int("dataLen", count), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("binary vector field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i := 0; i < count; i++ {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: binary vector field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("binary vector field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                       // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]]  // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                                // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.BinaryVectorFieldData) // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is binary vector type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is binary vector type, unequal to field %d", fieldID)
		}

		if fieldData.Dim != dim {
			log.Warn("Binlog adapter: binary vector dimension mismatch",
				zap.Int("sourceDim", dim), zap.Int("schemaDim", fieldData.Dim))
			return fmt.Errorf("binary vector dimension %d is not equal to schema dimension %d", dim, fieldData.Dim)
		}
		for j := 0; j < bytesPerVector; j++ {
			val := data[bytesPerVector*i+j]

			fieldData.Data = append(fieldData.Data, val)
		}
	}

	return nil
}

func (p *BinlogAdapter) dispatchFloatVecToShards(data []float32, dim int, memoryData []ShardData,
	shardList []int32, fieldID storage.FieldID) error {
	// verify row count
	count := len(data) / dim
	if count != len(shardList) {
		log.Warn("Binlog adapter: float vector field row count is not equal to shard list row count",
			zap.Int("dataLen", count), zap.Int("shardLen", len(shardList)))
		return fmt.Errorf("float vector field row count %d is not equal to shard list row count %d", len(data), len(shardList))
	}

	// dispatch entities acoording to shard list
	for i := 0; i < count; i++ {
		shardID := shardList[i]
		if shardID < 0 {
			continue // this entity has been deleted or excluded by timestamp
		}
		if shardID >= int32(len(memoryData)) {
			log.Warn("Binlog adapter: float vector field's shard ID is illegal", zap.Int32("shardID", shardID), zap.Int("shardsCount", len(memoryData)))
			return fmt.Errorf("float vector field's shard ID %d is larger than shards number %d", shardID, len(memoryData))
		}

		partitions := memoryData[shardID]                      // initBlockData() can ensure the existence, no need to check bound here
		fields := partitions[p.collectionInfo.PartitionIDs[0]] // NewBinlogAdapter() can ensure only one partition
		field := fields[fieldID]                               // initBlockData() can ensure the existence, no need to check existence here
		fieldData, ok := field.(*storage.FloatVectorFieldData) // avoid data type mismatch between binlog file and schema
		if !ok {
			log.Warn("Binlog adapter: binlog is float vector type, unequal to field",
				zap.Int64("fieldID", fieldID), zap.Int32("shardID", shardID))
			return fmt.Errorf("binlog is float vector type, unequal to field %d", fieldID)
		}

		if fieldData.Dim != dim {
			log.Warn("Binlog adapter: float vector dimension mismatch",
				zap.Int("sourceDim", dim), zap.Int("schemaDim", fieldData.Dim))
			return fmt.Errorf("binary vector dimension %d is not equal to schema dimension %d", dim, fieldData.Dim)
		}
		for j := 0; j < dim; j++ {
			val := data[dim*i+j]
			fieldData.Data = append(fieldData.Data, val)
		}
	}

	return nil
}
