package assignment

import (
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// newAssignmentDiscoverClient creates a new assignment discover client.
func newAssignmentDiscoverClient(w *watcher, streamClient streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverClient) *assignmentDiscoverClient {
	c := &assignmentDiscoverClient{
		lifetime:              typeutil.NewLifetime(),
		w:                     w,
		streamClient:          streamClient,
		logger:                log.With(),
		requestCh:             make(chan *streamingpb.AssignmentDiscoverRequest, 16),
		exitCh:                make(chan struct{}),
		wg:                    sync.WaitGroup{},
		lastErrorReportedTerm: make(map[string]int64),
	}
	c.executeBackgroundTask()
	return c
}

// assignmentDiscoverClient is the client for assignment discover.
type assignmentDiscoverClient struct {
	lifetime              *typeutil.Lifetime
	w                     *watcher
	logger                *log.MLogger
	requestCh             chan *streamingpb.AssignmentDiscoverRequest
	exitCh                chan struct{}
	wg                    sync.WaitGroup
	streamClient          streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverClient
	lastErrorReportedTerm map[string]int64
}

// ReportAssignmentError reports the assignment error to server.
func (c *assignmentDiscoverClient) ReportAssignmentError(pchannel types.PChannelInfo, err error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer c.lifetime.Done()

	statusErr := status.AsStreamingError(err).AsPBError()
	select {
	case c.requestCh <- &streamingpb.AssignmentDiscoverRequest{
		Command: &streamingpb.AssignmentDiscoverRequest_ReportError{
			ReportError: &streamingpb.ReportAssignmentErrorRequest{
				Pchannel: types.NewProtoFromPChannelInfo(pchannel),
				Err:      statusErr,
			},
		},
	}:
	case <-c.exitCh:
	}
}

func (c *assignmentDiscoverClient) IsAvailable() bool {
	select {
	case <-c.Available():
		return false
	default:
		return true
	}
}

// Available returns a channel that will be closed when the assignment discover client is available.
func (c *assignmentDiscoverClient) Available() <-chan struct{} {
	return c.exitCh
}

// Close closes the assignment discover client.
func (c *assignmentDiscoverClient) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	c.lifetime.Wait()

	close(c.requestCh)
	c.wg.Wait()
}

func (c *assignmentDiscoverClient) executeBackgroundTask() {
	c.wg.Add(2)
	go c.recvLoop()
	go c.sendLoop()
}

// sendLoop sends the request to server.
func (c *assignmentDiscoverClient) sendLoop() (err error) {
	defer c.wg.Done()
	for {
		req, ok := <-c.requestCh
		if !ok {
			// send close message and close send operation.
			if err := c.streamClient.Send(&streamingpb.AssignmentDiscoverRequest{
				Command: &streamingpb.AssignmentDiscoverRequest_Close{},
			}); err != nil {
				return err
			}
			return c.streamClient.CloseSend()
		}
		if c.shouldIgnore(req) {
			continue
		}
		if err := c.streamClient.Send(req); err != nil {
			return err
		}
	}
}

// shouldIgnore checks if the request should be ignored.
func (c *assignmentDiscoverClient) shouldIgnore(req *streamingpb.AssignmentDiscoverRequest) bool {
	switch req := req.Command.(type) {
	case *streamingpb.AssignmentDiscoverRequest_ReportError:
		if term, ok := c.lastErrorReportedTerm[req.ReportError.Pchannel.Name]; ok && req.ReportError.Pchannel.Term <= term {
			// If the error at newer term has been reported, ignore it right now.
			return true
		}
		c.lastErrorReportedTerm[req.ReportError.Pchannel.Name] = req.ReportError.Pchannel.Term
	}
	return false
}

// recvLoop receives the message from server.
// 1. FullAssignment
// 2. Close
func (c *assignmentDiscoverClient) recvLoop() (err error) {
	defer func() {
		c.wg.Done()
		close(c.exitCh)
	}()
	for {
		resp, err := c.streamClient.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch resp := resp.Response.(type) {
		case *streamingpb.AssignmentDiscoverResponse_FullAssignment:
			newIncomingVersion := typeutil.VersionInt64Pair{
				Global: resp.FullAssignment.Version.Global,
				Local:  resp.FullAssignment.Version.Local,
			}
			newIncomingAssignments := make(map[int64]types.StreamingNodeAssignment, len(resp.FullAssignment.Assignments))
			for _, assignment := range resp.FullAssignment.Assignments {
				channels := make(map[string]types.PChannelInfo, len(assignment.Channels))
				for _, channel := range assignment.Channels {
					channels[channel.Name] = types.NewPChannelInfoFromProto(channel)
				}
				newIncomingAssignments[assignment.GetNode().GetServerId()] = types.StreamingNodeAssignment{
					NodeInfo: types.NewStreamingNodeInfoFromProto(assignment.Node),
					Channels: channels,
				}
			}
			c.w.Update(types.VersionedStreamingNodeAssignments{
				Version:     newIncomingVersion,
				Assignments: newIncomingAssignments,
			})
		case *streamingpb.AssignmentDiscoverResponse_Close:
			// nothing to do now, just wait io.EOF.
		}
	}
}
