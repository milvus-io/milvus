package legacy

import "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"

const (
	DDOperationPrefixBefore220  = rootcoord.ComponentPrefix + "/dd-operation"
	DDMsgSendPrefixBefore220    = rootcoord.ComponentPrefix + "/dd-msg-send"
	IndexMetaBefore220Prefix    = rootcoord.ComponentPrefix + "/index"
	SegmentIndexPrefixBefore220 = rootcoord.ComponentPrefix + "/segment-index"
	IndexBuildPrefixBefore220   = "indexes"
	CollectionLoadMetaPrefixV1  = "queryCoord-collectionMeta"
)
