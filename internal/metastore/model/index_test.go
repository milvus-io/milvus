package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	indexID     = typeutil.UniqueID(1)
	indexName   = "idx"
	indexParams = []*commonpb.KeyValuePair{
		{
			Key:   "field110-i1",
			Value: "field110-v1",
		},
	}

	indexModel = &Index{
		IndexID:     indexID,
		IndexName:   indexName,
		IndexParams: indexParams,
		IsDeleted:   true,
		CreateTime:  1,
	}

	indexPb = &pb.IndexInfo{
		IndexName:   indexName,
		IndexID:     indexID,
		IndexParams: indexParams,
		Deleted:     true,
		CreateTime:  1,
	}
)

func TestMarshalIndexModel(t *testing.T) {
	ret := MarshalIndexModel(indexModel)
	assert.Equal(t, indexPb, ret)
	assert.Nil(t, MarshalIndexModel(nil))
}

func TestUnmarshalIndexModel(t *testing.T) {
	ret := UnmarshalIndexModel(indexPb)
	assert.Equal(t, indexModel, ret)
	assert.Nil(t, UnmarshalIndexModel(nil))
}
