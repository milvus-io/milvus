package helper

import (
	"github.com/milvus-io/milvus/client/v2/entity"
)

type CreateCollectionParams struct {
	CollectionFieldsType CollectionFieldsType // collection fields type
}

func NewCreateCollectionParams(collectionFieldsType CollectionFieldsType) *CreateCollectionParams {
	return &CreateCollectionParams{
		CollectionFieldsType: collectionFieldsType,
	}
}

type CreateCollectionOpt func(opt *createCollectionOpt)

type createCollectionOpt struct {
	shardNum             int32
	enabledDynamicSchema bool

	consistencyLevel entity.ConsistencyLevel
	properties       map[string]any
}

func TWithShardNum(shardNum int32) CreateCollectionOpt {
	return func(opt *createCollectionOpt) {
		opt.shardNum = shardNum
	}
}

func TWithProperties(properties map[string]any) CreateCollectionOpt {
	return func(opt *createCollectionOpt) {
		opt.properties = properties
	}
}
