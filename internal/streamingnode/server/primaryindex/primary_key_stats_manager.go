package primaryindex

import (
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type GrowingSegmentPKStatsManager struct {
	segmentStats map[int64]*storage.PrimaryKeyStats
	pkFieldID    int64
	pkType       int64

	mu sync.RWMutex
}

func NewGrowingSegmentPKStatsManager() *GrowingSegmentPKStatsManager {
	return &GrowingSegmentPKStatsManager{
		segmentStats: make(map[int64]*storage.PrimaryKeyStats),
		pkFieldID:    0,
		pkType:       0,
	}
}

func (gspm *GrowingSegmentPKStatsManager) CreateSegmentStats(segmentID int64, pkFieldID int64, pkType int64, estimatedRowCount int64) error {
	gspm.mu.Lock()
	defer gspm.mu.Unlock()

	if _, exists := gspm.segmentStats[segmentID]; !exists {
		stats, err := storage.NewPrimaryKeyStats(pkFieldID, pkType, estimatedRowCount)
		if err != nil {
			log.Error("failed to create primary key stats for segment",
				zap.Int64("segmentID", segmentID),
				zap.Error(err))
			return err
		}

		gspm.segmentStats[segmentID] = stats
		log.Info("created primary key stats for growing segment",
			zap.Int64("segmentID", segmentID),
			zap.Int64("pkFieldID", pkFieldID))
	}

	return nil
}

func (gspm *GrowingSegmentPKStatsManager) UpdatePrimaryKey(segmentID int64, pk storage.PrimaryKey) error {
	gspm.mu.Lock()
	defer gspm.mu.Unlock()

	stats, exists := gspm.segmentStats[segmentID]
	if !exists {
		log.Warn("primary key stats not found for segment", zap.Int64("segmentID", segmentID))
		return nil
	}

	stats.Update(pk)
	return nil
}

func (gspm *GrowingSegmentPKStatsManager) BatchUpdatePrimaryKeys(segmentID int64, pks []storage.PrimaryKey) error {
	gspm.mu.Lock()
	defer gspm.mu.Unlock()

	stats, exists := gspm.segmentStats[segmentID]
	if !exists {
		log.Warn("primary key stats not found for segment", zap.Int64("segmentID", segmentID))
		return nil
	}

	for _, pk := range pks {
		stats.Update(pk)
	}

	log.Debug("batch updated primary key stats for segment",
		zap.Int64("segmentID", segmentID),
		zap.Int("count", len(pks)))

	return nil
}

func (gspm *GrowingSegmentPKStatsManager) CheckPrimaryKeyExists(pk storage.PrimaryKey) int64 {
	gspm.mu.RLock()
	defer gspm.mu.RUnlock()

	for segmentID, stats := range gspm.segmentStats {
		if stats == nil {
			continue
		}

		var result bool
		switch pk.Type() {
		case schemapb.DataType_Int64:
			buf := make([]byte, 8)
			int64Value := pk.(*storage.Int64PrimaryKey).Value
			common.Endian.PutUint64(buf, uint64(int64Value))
			result = stats.BF.Test(buf)
		case schemapb.DataType_VarChar:
			stringValue := pk.(*storage.VarCharPrimaryKey).Value
			result = stats.BF.TestString(stringValue)
		default:
			result = false
		}

		if result {
			return segmentID
		}
	}

	return -1
}

func (gspm *GrowingSegmentPKStatsManager) GetAllStats() map[int64]*storage.PrimaryKeyStats {
	gspm.mu.RLock()
	defer gspm.mu.RUnlock()

	stats := make(map[int64]*storage.PrimaryKeyStats)
	for segmentID, stat := range gspm.segmentStats {
		stats[segmentID] = stat
	}

	return stats
}

func (gspm *GrowingSegmentPKStatsManager) RemoveSegmentStats(segmentID int64) {
	gspm.mu.Lock()
	defer gspm.mu.Unlock()

	if _, exists := gspm.segmentStats[segmentID]; exists {
		delete(gspm.segmentStats, segmentID)
		log.Info("removed primary key stats for segment", zap.Int64("segmentID", segmentID))
	}
}

func (gspm *GrowingSegmentPKStatsManager) GetStatsSummary() map[string]interface{} {
	gspm.mu.RLock()
	defer gspm.mu.RUnlock()

	totalMemory := uint64(0)
	for _, stats := range gspm.segmentStats {
		if stats.BF != nil {
			totalMemory += uint64(stats.BF.Cap())
		}
	}

	return map[string]interface{}{
		"totalSegments": len(gspm.segmentStats),
		"totalMemory":   totalMemory,
	}
}

func (gspm *GrowingSegmentPKStatsManager) CheckDuplicatePrimaryKeys(pks []storage.PrimaryKey) *schemapb.IDs {
	gspm.mu.RLock()
	defer gspm.mu.RUnlock()
	var duplicates []storage.PrimaryKey
	for _, pk := range pks {
		for _, stats := range gspm.segmentStats {
			if stats == nil {
				continue
			}
			if stats.BF != nil {
				var exists bool
				switch pk.Type() {
				case schemapb.DataType_Int64:
					buf := make([]byte, 8)
					int64Value := pk.(*storage.Int64PrimaryKey).Value
					common.Endian.PutUint64(buf, uint64(int64Value))
					exists = stats.BF.Test(buf)
				case schemapb.DataType_VarChar:
					stringValue := pk.(*storage.VarCharPrimaryKey).Value
					exists = stats.BF.TestString(stringValue)
				}
				if exists {
					duplicates = append(duplicates, pk)
					break
				}
			}
		}
	}
	ids := storage.ParsePrimaryKeys2IDs(duplicates)
	return ids
}

func (gspm *GrowingSegmentPKStatsManager) SetPrimaryKeyInfo(pkFieldID int64, pkType int64) {
	gspm.mu.Lock()
	defer gspm.mu.Unlock()
	gspm.pkFieldID = pkFieldID
	gspm.pkType = pkType
	log.Info("set primary key info", zap.Int64("pkFieldID", pkFieldID), zap.Int64("pkType", pkType))
}

func (gspm *GrowingSegmentPKStatsManager) GetPrimaryKeyInfo() (int64, int64) {
	gspm.mu.RLock()
	defer gspm.mu.RUnlock()
	return gspm.pkFieldID, gspm.pkType
}

func (gspm *GrowingSegmentPKStatsManager) ConvertToPrimaryKeys(data []interface{}) []storage.PrimaryKey {
	pks := make([]storage.PrimaryKey, len(data))
	for i, v := range data {
		switch val := v.(type) {
		case int64:
			pks[i] = storage.NewInt64PrimaryKey(val)
		case string:
			pks[i] = storage.NewVarCharPrimaryKey(val)
		default:
			log.Warn("unsupported primary key type", zap.Any("value", v))
		}
	}
	return pks
}

func (gspm *GrowingSegmentPKStatsManager) ExtractColumnData(insertMsg interface{}, fieldID int64) ([]interface{}, error) {
	msg, ok := insertMsg.(interface {
		Body() (*msgpb.InsertRequest, error)
	})
	if !ok {
		return nil, errors.New("invalid insert message type")
	}

	body, err := msg.Body()
	if err != nil {
		return nil, err
	}

	var fieldData *schemapb.FieldData
	for _, fd := range body.FieldsData {
		if fd.FieldId == fieldID {
			fieldData = fd
			break
		}
	}

	if fieldData == nil {
		return nil, errors.New("field not found")
	}

	switch fieldData.Type {
	case schemapb.DataType_Int64:
		if intData := fieldData.GetScalars().GetIntData(); intData != nil {
			result := make([]interface{}, len(intData.Data))
			for i, value := range intData.Data {
				result[i] = int64(value)
			}
			return result, nil
		}
	case schemapb.DataType_VarChar:
		if stringData := fieldData.GetScalars().GetStringData(); stringData != nil {
			result := make([]interface{}, len(stringData.Data))
			for i, value := range stringData.Data {
				result[i] = value
			}
			return result, nil
		}
	default:
		return nil, errors.Errorf("unsupported data type: %s", fieldData.Type.String())
	}

	return nil, errors.New("no data found for field")
}

func (gspm *GrowingSegmentPKStatsManager) ExtractPrimaryKeyColumn(insertMsg interface{}) ([]interface{}, error) {
	pkFieldID, _ := gspm.GetPrimaryKeyInfo()
	if pkFieldID == 0 {
		return nil, errors.New("primary key field ID not set")
	}

	data, err := gspm.ExtractColumnData(insertMsg, pkFieldID)
	if err != nil {
		log.Warn("failed to extract primary key column",
			zap.Int64("fieldID", pkFieldID),
			zap.Error(err))
		return nil, err
	}

	log.Info("extracted primary key column data",
		zap.Int64("fieldID", pkFieldID),
		zap.Int("count", len(data)))

	return data, nil
}

func (gspm *GrowingSegmentPKStatsManager) UpdateBloomFilterFromInsert(insertMsg interface{}, segmentID int64) {
	pks, err := gspm.ExtractPrimaryKeyColumn(insertMsg)
	if err != nil {
		log.Warn("failed to extract primary key column", zap.Error(err))
		return
	}

	if len(pks) > 0 {
		primaryKeys := gspm.ConvertToPrimaryKeys(pks)
		if err := gspm.BatchUpdatePrimaryKeys(segmentID, primaryKeys); err != nil {
			log.Warn("failed to update primary key stats for segment",
				zap.Int64("segmentID", segmentID),
				zap.Int("pkCount", len(primaryKeys)),
				zap.Error(err))
		} else {
			log.Info("updated Bloom Filter for segment",
				zap.Int64("segmentID", segmentID),
				zap.Int("pkCount", len(primaryKeys)))
		}
	}
}
