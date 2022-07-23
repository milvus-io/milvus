package table

import (
	"encoding/json"
	"time"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"go.uber.org/zap"
)

type Index struct {
	ID           int64     `db:"id"`
	TenantID     *string   `db:"tenant_id"`
	FieldID      int64     `db:"field_id"`
	CollectionID int64     `db:"collection_id"`
	IndexID      int64     `db:"index_id"`
	IndexName    string    `db:"index_name"`
	IndexParams  string    `db:"index_params"`
	IsDeleted    bool      `db:"is_deleted"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

// model <---> db

func ConvertIndexDBToModel(index *Index) common.Int64Tuple {
	var indexParams []commonpb.KeyValuePair
	if index.IndexParams != "" {
		err := json.Unmarshal([]byte(index.IndexParams), &indexParams)
		if err != nil {
			log.Error("unmarshal IndexParams of field failed", zap.String("IndexParams", index.IndexParams), zap.Error(err))
		}
	}
	return common.Int64Tuple{
		Key:   index.FieldID,
		Value: index.IndexID,
	}
}

func ConvertToIndexModel(index *Index, segmentIndex *SegmentIndex) *model.Index {
	var indexParams []commonpb.KeyValuePair
	err := json.Unmarshal([]byte(index.IndexParams), &indexParams)
	if err != nil {
		log.Error("unmarshal IndexParams of field failed", zap.Error(err))
	}
	segIndex := ConvertSegmentIndexDBToModel(segmentIndex)
	return &model.Index{
		CollectionID: index.CollectionID,
		FieldID:      index.FieldID,
		IndexID:      index.IndexID,
		IndexName:    index.IndexName,
		IndexParams:  funcutil.ConvertToKeyValuePairPointer(indexParams),
		SegmentIndexes: map[int64]model.SegmentIndex{
			segIndex.SegmentID: *segIndex,
		},
	}
}

func ConvertIndexesToMap(input []struct {
	Index
	SegmentIndex
}) map[string]*model.Index {
	idxMap := make(map[string]*model.Index)
	for _, record := range input {
		c := ConvertToIndexModel(&record.Index, &record.SegmentIndex)
		idxMap[c.IndexName] = c
	}
	return idxMap
}
