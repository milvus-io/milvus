package model

import pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

type Alias struct {
	Name         string
	CollectionID int64
	CreatedTime  uint64
}

func MarshalAliasModel(alias *Alias) *pb.AliasInfo {
	return &pb.AliasInfo{
		AliasName:    alias.Name,
		CollectionId: alias.CollectionID,
		CreatedTime:  alias.CreatedTime,
	}
}

func UnmarshalAliasModel(info *pb.AliasInfo) *Alias {
	return &Alias{
		Name:         info.GetAliasName(),
		CollectionID: info.GetCollectionId(),
		CreatedTime:  info.GetCreatedTime(),
	}
}
