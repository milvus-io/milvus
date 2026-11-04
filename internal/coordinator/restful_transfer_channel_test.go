package coordinator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestTransferStreamingChannel(t *testing.T) {
	coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
	var received querypb.TransferChannelRequest
	var transferErr error
	patch := mockey.Mock((*querycoordv2.Server).TransferChannel).To(func(_ *querycoordv2.Server, _ context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
		// Copy scalar request fields while the caller's stack frame is live.
		received = querypb.TransferChannelRequest{
			SourceNodeID: req.GetSourceNodeID(), TargetNodeID: req.GetTargetNodeID(),
			ChannelName: req.GetChannelName(), CopyMode: req.GetCopyMode(),
			TransferAll: req.GetTransferAll(), ToAllNodes: req.GetToAllNodes(),
		}
		return merr.Status(transferErr), nil
	}).Build()
	defer patch.UnPatch()

	invoke := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		coord.TransferStreamingChannel(w, httptest.NewRequest(http.MethodPost, "/management/streaming/transfer", strings.NewReader(body)))
		return w
	}
	w := invoke(`{"source_node_id":11,"target_node_id":12,"channel_name":"p1_100v0"}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, int64(11), received.SourceNodeID)
	require.Equal(t, int64(12), received.TargetNodeID)
	require.Equal(t, "p1_100v0", received.ChannelName)
	require.False(t, received.CopyMode)
	require.False(t, received.TransferAll)
	require.False(t, received.ToAllNodes)
	w = invoke(`{"source_node_id":11}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, received.TransferAll)
	require.True(t, received.ToAllNodes)

	transferErr = merr.ErrServiceUnavailable
	w = invoke(`{"source_node_id":11}`)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, w.Body.String(), "failed to transfer channel")
}

func TestTransferStreamingChannelWaits(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered, confirmed := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*querycoordv2.Server).TransferChannel).To(func(_ *querycoordv2.Server, ctx context.Context, _ *querypb.TransferChannelRequest) (*commonpb.Status, error) {
		close(entered)
		select {
		case <-confirmed:
			return merr.Success(), nil
		case <-ctx.Done():
			return merr.Status(ctx.Err()), nil
		}
	}).Build()
	defer patch.UnPatch()
	coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
	w := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/management/streaming/transfer", strings.NewReader(`{"source_node_id":11}`)).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		coord.TransferStreamingChannel(w, request)
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-done:
		t.Fatal("REST returned before WAL transfer completed")
	default:
	}
	close(confirmed)
	select {
	case <-done:
		require.Equal(t, http.StatusOK, w.Code)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
