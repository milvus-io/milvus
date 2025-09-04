package primaryindex

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type PKStatsManager struct {
	growingSegmentPK  map[string]map[int64]*storage.PrimaryKeyStats
	pkFieldID         int64
	pkType            int64
	sealedIndexHandle map[string]*indexcgowrapper.PrimaryIndexHandle
	mu                sync.RWMutex
}

func NewPKStatsManager() *PKStatsManager {
	return &PKStatsManager{
		growingSegmentPK:  make(map[string]map[int64]*storage.PrimaryKeyStats),
		sealedIndexHandle: make(map[string]*indexcgowrapper.PrimaryIndexHandle),
		pkFieldID:         0,
		pkType:            0,
	}
}

func (pksm *PKStatsManager) createSegmentStats(vchannelName string, segmentID int64, pkFieldID int64, pkType int64) *storage.PrimaryKeyStats {
	if pksm.growingSegmentPK[vchannelName] == nil {
		pksm.growingSegmentPK[vchannelName] = make(map[int64]*storage.PrimaryKeyStats)
	}

	stats, err := storage.NewPrimaryKeyStats(pkFieldID, pkType, 200000)
	if err != nil {
		log.Error("failed to create primary key stats for segment",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return nil
	}

	pksm.growingSegmentPK[vchannelName][segmentID] = stats
	log.Info("created primary key stats for growing segment",
		zap.Int64("segmentID", segmentID),
		zap.Int64("pkFieldID", pkFieldID))

	return stats
}

func (pksm *PKStatsManager) BatchUpdatePrimaryKeys(vchannelName string, segmentID int64, pks []storage.PrimaryKey) error {
	pksm.mu.Lock()
	defer pksm.mu.Unlock()

	stats, exists := pksm.growingSegmentPK[vchannelName][segmentID]
	if !exists {
		stats = pksm.createSegmentStats(vchannelName, segmentID, pksm.pkFieldID, pksm.pkType)
		if stats == nil {
			return errors.New("failed to create primary key stats for segment")
		}
	}

	for _, pk := range pks {
		stats.Update(pk)
	}

	log.Debug("batch updated primary key stats for segment",
		zap.Int64("segmentID", segmentID),
		zap.Int("count", len(pks)))

	return nil
}

func (pksm *PKStatsManager) RemoveSegmentStats(vchannelName string, segmentID int64) {
	pksm.mu.Lock()
	defer pksm.mu.Unlock()

	if _, exists := pksm.growingSegmentPK[vchannelName][segmentID]; exists {
		delete(pksm.growingSegmentPK[vchannelName], segmentID)
		log.Info("removed primary key stats for segment", zap.Int64("segmentID", segmentID))
	}
}

func (pksm *PKStatsManager) Close() {
	pksm.mu.Lock()
	defer pksm.mu.Unlock()

	for _, handle := range pksm.sealedIndexHandle {
		handle.Close()
	}

	pksm.growingSegmentPK = nil
}

func (pksm *PKStatsManager) CheckDuplicatePrimaryKeys(vchannelName string, pks []storage.PrimaryKey) (*schemapb.IDs, []int64) {
	pksm.mu.RLock()
	defer pksm.mu.RUnlock()
	var duplicates []storage.PrimaryKey
	var segmentIDs []int64
	for _, pk := range pks {
		for segmentID, stats := range pksm.growingSegmentPK[vchannelName] {
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
					log.Debug("check duplicate primary keys", zap.Any("pk", pk), zap.Int64("segmentID", segmentID))
					duplicates = append(duplicates, pk)
					segmentIDs = append(segmentIDs, segmentID)
				}
			}
		}
		if pksm.sealedIndexHandle[vchannelName] != nil {
			result, err := pksm.sealedIndexHandle[vchannelName].Query(pk.(*storage.VarCharPrimaryKey).Value)
			if err != nil {
				log.Error("failed to query sealed index", zap.Error(err))
				continue
			}
			log.Debug("query sealed index", zap.Int64("result", result))
			if result != -1 {
				duplicates = append(duplicates, pk)
				segmentIDs = append(segmentIDs, result)
			}
		}
	}
	log.Info("check duplicate primary keys", zap.Int("duplicates", len(duplicates)), zap.Int("segmentIDs", len(segmentIDs)),
		zap.Any("duplicates", duplicates),
		zap.Any("pks", pks))

	ids := storage.ParsePrimaryKeys2IDs(duplicates)
	return ids, segmentIDs
}

func (pksm *PKStatsManager) SetPrimaryKeyInfo(pkFieldID int64, pkType int64) {
	pksm.mu.Lock()
	defer pksm.mu.Unlock()
	pksm.pkFieldID = pkFieldID
	pksm.pkType = pkType
	log.Info("set primary key info", zap.Int64("pkFieldID", pkFieldID), zap.Int64("pkType", pkType))
}

func (pksm *PKStatsManager) GetPrimaryKeyInfo() (int64, int64) {
	pksm.mu.RLock()
	defer pksm.mu.RUnlock()
	return pksm.pkFieldID, pksm.pkType
}

func (pksm *PKStatsManager) convertToPrimaryKeys(data []interface{}) []storage.PrimaryKey {
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

func (pksm *PKStatsManager) ExtractColumnData(insertMsg interface{}, fieldID int64) ([]interface{}, error) {
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

func (pksm *PKStatsManager) ExtractPrimaryKeyColumn(insertMsg interface{}) ([]storage.PrimaryKey, error) {
	pkFieldID, _ := pksm.GetPrimaryKeyInfo()
	if pkFieldID == 0 {
		return nil, errors.New("primary key field ID not set")
	}

	data, err := pksm.ExtractColumnData(insertMsg, pkFieldID)
	if err != nil {
		log.Warn("failed to extract primary key column",
			zap.Int64("fieldID", pkFieldID),
			zap.Error(err))
		return nil, err
	}

	log.Debug("extracted primary key column data",
		zap.Int64("fieldID", pkFieldID),
		zap.Int("count", len(data)))

	return pksm.convertToPrimaryKeys(data), nil
}

func (pksm *PKStatsManager) UpdateBloomFilterFromPrimaryKeys(vchannelName string, primaryKeys []storage.PrimaryKey, segmentID int64) {
	if err := pksm.BatchUpdatePrimaryKeys(vchannelName, segmentID, primaryKeys); err != nil {
		log.Warn("failed to update primary key stats for segment",
			zap.Int64("segmentID", segmentID),
			zap.Int("pkCount", len(primaryKeys)),
			zap.Error(err))
	}
}

func (pksm *PKStatsManager) LoadSealedIndex(eventData *datapb.PrimaryKeyIndexBuiltData) {
	pksm.mu.Lock()
	defer pksm.mu.Unlock()
	log.Info("loading sealed index", zap.String("vchannelName", eventData.VchannelName), zap.Int64("version", eventData.Version), zap.Int64("collectionID", eventData.CollectionId), zap.Strings("files", eventData.Files))
	if pksm.sealedIndexHandle[eventData.VchannelName] != nil {
		pksm.sealedIndexHandle[eventData.VchannelName].Close()
	}

	loadParams := &indexcgopb.LoadPrimaryIndexInfo{
		BuildID:      eventData.Version,
		CollectionID: eventData.CollectionId,
		Files:        eventData.Files,
	}

	handle, err := indexcgowrapper.LoadPrimaryIndex(context.Background(), loadParams)
	if err != nil {
		log.Error("failed to load sealed segment index", zap.Error(err))
		return
	}

	pksm.sealedIndexHandle[eventData.VchannelName] = handle

	segmentList, err := handle.GetSegmentList()
	if err != nil {
		log.Error("failed to get segment list", zap.Error(err))
		return
	}
	for _, segmentID := range segmentList {
		if _, exists := pksm.growingSegmentPK[eventData.VchannelName][segmentID]; exists {
			log.Info("delete growing segment pk", zap.Int64("segmentID", segmentID), zap.String("vchannelName", eventData.VchannelName), zap.Int("length", len(pksm.growingSegmentPK[eventData.VchannelName])))
			delete(pksm.growingSegmentPK[eventData.VchannelName], segmentID)
			log.Info("after delete growing segment pk", zap.Int64("segmentID", segmentID), zap.String("vchannelName", eventData.VchannelName), zap.Int("length", len(pksm.growingSegmentPK[eventData.VchannelName])), zap.Int("length", len(pksm.growingSegmentPK[eventData.VchannelName])))
		}
	}

	log.Info("loaded sealed segment index",
		zap.Int64("collectionID", eventData.CollectionId),
		zap.String("vchannel", eventData.VchannelName),
		zap.Strings("files", eventData.Files))
}

func (pksm *PKStatsManager) SegmentChange(eventData *datapb.SegmentCompactionData) {
	pksm.mu.Lock()
	defer pksm.mu.Unlock()
	for _, seg := range eventData.Infos {
		if pksm.growingSegmentPK[seg.GetInsertChannel()] != nil {
			for _, segmentIDfrom := range seg.GetCompactionFrom() {
				log.Info("segment change", zap.Int64("segmentID", seg.GetID()), zap.Int64("segmentIDfrom", segmentIDfrom), zap.String("vchannelName", seg.GetInsertChannel()),
					zap.Int("length", len(pksm.growingSegmentPK[seg.GetInsertChannel()])))
				if _, exists := pksm.growingSegmentPK[seg.GetInsertChannel()][segmentIDfrom]; exists {
					pksm.growingSegmentPK[seg.GetInsertChannel()][seg.GetID()] = pksm.growingSegmentPK[seg.GetInsertChannel()][segmentIDfrom]
					delete(pksm.growingSegmentPK[seg.GetInsertChannel()], segmentIDfrom)
				}
				log.Info("after delete segment change", zap.Int64("segmentID", seg.GetID()), zap.Int64("segmentIDfrom", segmentIDfrom), zap.String("vchannelName", seg.GetInsertChannel()),
					zap.Int("length", len(pksm.growingSegmentPK[seg.GetInsertChannel()])))
			}
		}
		if pksm.sealedIndexHandle[seg.GetInsertChannel()] != nil {
			for _, segmentIDfrom := range seg.GetCompactionFrom() {
				pksm.sealedIndexHandle[seg.GetInsertChannel()].ResetSegmentId(seg.GetID(), segmentIDfrom)
			}
		}
		log.Info("growingSegmentPK change done")
	}
}
