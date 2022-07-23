package table

import (
	"encoding/json"
	"time"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type Collection struct {
	ID               int64              `db:"id"`
	TenantID         *string            `db:"tenant_id"`
	CollectionID     int64              `db:"collection_id"`
	CollectionName   string             `db:"collection_name"`
	Description      *string            `db:"description"`
	AutoID           bool               `db:"auto_id"`
	ShardsNum        int32              `db:"num_shards"`
	StartPosition    string             `db:"start_position"`
	ConsistencyLevel int32              `db:"consistency_level"`
	Ts               typeutil.Timestamp `db:"ts"`
	IsDeleted        bool               `db:"is_deleted"`
	CreatedAt        time.Time          `db:"created_at"`
	UpdatedAt        time.Time          `db:"updated_at"`
}

// model <---> db

func ConvertCollectionDBToModel(coll *Collection, collChannel *CollectionChannel, partition *Partition, field *Field, index *Index) *model.Collection {
	var retDescription string
	if coll.Description != nil {
		retDescription = *coll.Description
	}

	var retFields []*model.Field
	retFields = append(retFields, ConvertFieldDBToModel(field))

	var retPartitions []*model.Partition
	retPartitions = append(retPartitions, ConvertPartitionDBToModel(partition))

	var retIndexes []common.Int64Tuple
	retIndexes = append(retIndexes, ConvertIndexDBToModel(index))

	var startPositions []*commonpb.KeyDataPair
	if coll.StartPosition != "" {
		err := json.Unmarshal([]byte(coll.StartPosition), &startPositions)
		if err != nil {
			log.Error("unmarshal collection start positions error", zap.Error(err))
		}
	}

	return &model.Collection{
		CollectionID:         coll.CollectionID,
		Name:                 coll.CollectionName,
		Description:          retDescription,
		AutoID:               coll.AutoID,
		Fields:               retFields,
		Partitions:           retPartitions,
		FieldIDToIndexID:     retIndexes,
		VirtualChannelNames:  []string{collChannel.VirtualChannel},
		PhysicalChannelNames: []string{collChannel.PhysicalChannel},
		ShardsNum:            coll.ShardsNum,
		StartPositions:       startPositions,
		ConsistencyLevel:     commonpb.ConsistencyLevel(coll.ConsistencyLevel),
		CreateTime:           coll.Ts,
	}
}

func ConvertCollectionsToIDMap(colls []*model.Collection) map[typeutil.UniqueID]*model.Collection {
	colMap := make(map[typeutil.UniqueID]*model.Collection)
	for _, c := range colls {
		if existColl, ok := colMap[c.CollectionID]; !ok {
			colMap[c.CollectionID] = c
		} else {
			existColl.Fields = append(existColl.Fields, c.Fields...)
			existColl.Partitions = append(existColl.Partitions, c.Partitions...)
			existColl.FieldIDToIndexID = append(existColl.FieldIDToIndexID, c.FieldIDToIndexID...)
			existColl.VirtualChannelNames = append(existColl.VirtualChannelNames, c.VirtualChannelNames...)
			existColl.PhysicalChannelNames = append(existColl.PhysicalChannelNames, c.PhysicalChannelNames...)
		}
	}
	return colMap
}

func ConvertCollectionsToNameMap(colls []*model.Collection) map[string]*model.Collection {
	colMap := make(map[string]*model.Collection)
	for _, c := range colls {
		if existColl, ok := colMap[c.Name]; !ok {
			colMap[c.Name] = c
		} else {
			existColl.Fields = append(existColl.Fields, c.Fields...)
			existColl.Partitions = append(existColl.Partitions, c.Partitions...)
			existColl.FieldIDToIndexID = append(existColl.FieldIDToIndexID, c.FieldIDToIndexID...)
		}
	}
	return colMap
}
