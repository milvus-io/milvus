package querynode

import (
	"context"

	"github.com/minio/minio-go/v7"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type LoadIndex struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *minio.Client

	replica                       collectionReplica
	numCompletedSegmentsToFieldID map[int64]int64

	msgBuffer          chan msgstream.TsMsg
	unsolvedMsg        []msgstream.TsMsg
	loadIndexMsgStream msgstream.MsgStream

	queryNodeID UniqueID
}

func (li *LoadIndex) loadIndex(indexKey []string) [][]byte {
	// TODO:: load dataStore client interface to load builtIndex according index key

	return nil
}

func (li *LoadIndex) updateSegmentIndex(bytesIndex [][]byte, segmentID UniqueID) error {
	// TODO:: dataStore return bytes index, load index to c++ segment
	// TODO: how to deserialize bytes to segment index?

	return nil
}

func (li *LoadIndex) sendQueryNodeStats() error {
	// TODO:: update segment index type in replica, and publish queryNode segmentStats
	return nil
}
