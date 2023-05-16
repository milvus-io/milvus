package rootcoord

import (
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type collectionDb struct {
	collID2Meta map[typeutil.UniqueID]*model.Collection // collection id -> collection meta
}

func newCollectionDb() *collectionDb {
	return &collectionDb{
		collID2Meta: make(map[typeutil.UniqueID]*model.Collection),
	}
}
