package client

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

var (
	ErrNodeGone = errors.New("underlying work node is gone") // When the underlying work node is gone, the error will be returned, it can never be recovery.

	_ SyncMessage = SyncResponseMessage{}
	_ SyncMessage = SyncErrorMessage{}
)

// SyncMessage is the interface to represent the message from the node.
type SyncMessage interface {
	GetWorkNode() qviews.WorkNode

	isSyncMessage()
}

// SyncResponseMessage is the message to represent the response from the node.
type SyncResponseMessage struct {
	WorkNode qviews.WorkNode
	Response *viewpb.SyncQueryViewsResponse
}

func (m SyncResponseMessage) isSyncMessage() {}

func (m SyncResponseMessage) GetWorkNode() qviews.WorkNode {
	return m.WorkNode
}

// SyncErrorMessage is the message to represent the error from the node.
// The error should be handled anyway.
type SyncErrorMessage struct {
	WorkNode qviews.WorkNode
	Error    error
}

func (m SyncErrorMessage) isSyncMessage() {}

func (m SyncErrorMessage) GetWorkNode() qviews.WorkNode {
	return m.WorkNode
}

// SyncOption is the option to create syncer.
type SyncOption struct {
	// WorkNode is the target work node to send the sync request.
	WorkNode qviews.WorkNode
	// Receiver is the channel to receive the various message from the node.
	// When the sync operation is done or error happens, the SyncErrorMessage will be received.
	// SyncResponseMessage -> SyncResponseMessage -> SyncErrorMessage.
	Receiver chan<- SyncMessage
}

// QueryViewServiceClient is the interface to send sync request to the view service.
type QueryViewServiceClient interface {
	log.LoggerBinder

	// CreateSyncer create a syncer to send sync request to the related node.
	// Various goroutines will be created to handle the sync operation at background.
	CreateSyncer(opt SyncOption) QueryViewServiceSyncer
}

// QueryViewServiceSyncer is the interface to send sync request to the view service.
// And keep receiving the response from the node.
type QueryViewServiceSyncer interface {
	// SyncAtBackground sent the sync request to the related node.
	// But it doesn't promise the sync operation is done at server-side.
	// Make sure the sync operation is done by the Receiving message.
	SyncAtBackground(*viewpb.SyncQueryViewsRequest)

	// Close the client.
	// syncer will receive the ErrClosed error.
	Close()
}
