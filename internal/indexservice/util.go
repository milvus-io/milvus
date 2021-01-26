package indexservice

import "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

func CompareAddress(a *commonpb.Address, b *commonpb.Address) bool {
	if a == b {
		return true
	}
	return a.Ip == b.Ip && a.Port == b.Port
}
