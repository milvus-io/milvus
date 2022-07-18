package querycoord

import "time"

const (
	collectionMetaPrefix   = "queryCoord-collectionMeta"
	dmChannelMetaPrefix    = "queryCoord-dmChannelWatchInfo"
	deltaChannelMetaPrefix = "queryCoord-deltaChannel"
	ReplicaMetaPrefix      = "queryCoord-ReplicaMeta"

	// TODO, we shouldn't separate querycoord tasks to 3 meta keys, there should only one with different states, otherwise there will be a high possibility to be inconsitent
	triggerTaskPrefix = "queryCoord-triggerTask"
	activeTaskPrefix  = "queryCoord-activeTask"
	taskInfoPrefix    = "queryCoord-taskInfo"

	queryNodeInfoPrefix = "queryCoord-queryNodeInfo"
	// TODO, remove unsubscribe
	unsubscribeChannelInfoPrefix = "queryCoord-unsubscribeChannelInfo"
	timeoutForRPC                = 10 * time.Second
	// MaxSendSizeToEtcd is the default limit size of etcd messages that can be sent and received
	// MaxSendSizeToEtcd = 2097152
	// Limit size of every loadSegmentReq to 200k
	MaxSendSizeToEtcd = 200000
)
