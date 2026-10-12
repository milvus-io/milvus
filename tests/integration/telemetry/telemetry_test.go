// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package telemetry covers the client-telemetry round trip: an SDK client reporting what it
// did, and the coordinator pushing commands back to it.
//
// Both directions need a live client and a live cluster at once, which is why unit tests
// cannot reach them: the client-side suite mocks the server and the server-side suite mocks
// the client, so the wire between them -- heartbeat carrying metrics, commands riding the
// heartbeat response, replies riding the next one -- was covered by nothing until this.
//
// Timing is the trap here. A heartbeat carries only the operations since the previous one,
// and the client resets its counters as it takes that snapshot, so any assertion that runs
// load, stops, and then looks is a coin flip on which window the query lands in. Every
// assertion below therefore either keeps issuing load while it polls, or waits on something
// that does not expire -- never "sleep one interval, then check".
package telemetry

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim         = 8
	rowNum      = 200
	vectorField = integration.FloatVecField
	heartbeat   = 2 * time.Second
	// retainedWindows is raised from the default 2 so that the survival of a quiet interval
	// can be asserted without racing the heartbeat: with N windows retained, the reported
	// one stays put for N-1 intervals after traffic stops.
	retainedWindows = 8
	pollTimeout     = 90 * time.Second
	pollInterval    = 500 * time.Millisecond
	loadedCollName  = "telemetry_round_trip"
)

type TelemetrySuite struct {
	integration.MiniClusterSuite

	sdk *milvusclient.Client
}

func (s *TelemetrySuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().RootCoordCfg.ClientTelemetryRetainedWindows.Key, strconv.Itoa(retainedWindows))
	s.MiniClusterSuite.SetupSuite()
}

func (s *TelemetrySuite) SetupTest() {
	s.MiniClusterSuite.SetupTest()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           "",
		CollectionName:   loadedCollName,
		ChannelNum:       1,
		SegmentNum:       1,
		RowNumPerSegment: rowNum,
		Dim:              dim,
	})
	s.WaitForIndexBuilt(ctx, loadedCollName, vectorField)

	// A short heartbeat keeps the test's polling loops short. Sampling is left at 1.0 so
	// every operation is counted and the assertions can be about presence rather than luck.
	sdk, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: "localhost:19530",
		TelemetryConfig: &milvusclient.TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: heartbeat,
			SamplingRate:      1.0,
			ErrorMaxCount:     100,
		},
	})
	s.Require().NoError(err)
	s.sdk = sdk

	loadTask, err := s.sdk.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(loadedCollName))
	s.Require().NoError(err)
	s.Require().NoError(loadTask.Await(ctx))
}

func (s *TelemetrySuite) TearDownTest() {
	if s.sdk != nil {
		_ = s.sdk.Close(context.Background())
		s.sdk = nil
	}
	s.MiniClusterSuite.TearDownTest()
}

// search issues one Search through the SDK, which is what records a telemetry operation --
// the raw MilvusServiceClient the rest of the harness uses does not.
func (s *TelemetrySuite) search(ctx context.Context) {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i)
	}
	_, err := s.sdk.Search(ctx, milvusclient.NewSearchOption(loadedCollName, 1, []entity.Vector{entity.FloatVector(vec)}))
	s.Require().NoError(err)
}

// pollWhileSearching keeps issuing searches until check passes, so the window being reported
// is never an idle one. Returns the condition's last observation for a clearer failure.
func (s *TelemetrySuite) pollWhileSearching(ctx context.Context, what string, check func() bool) {
	deadline := time.Now().Add(pollTimeout)
	for time.Now().Before(deadline) {
		s.search(ctx)
		if check() {
			return
		}
		time.Sleep(pollInterval)
	}
	s.FailNow(fmt.Sprintf("timed out after %v waiting for %s", pollTimeout, what))
}

// searchRequestsFor returns the Search request count the coordinator currently reports for
// this client, or 0 when it reports no Search window at all.
func (s *TelemetrySuite) searchRequestsFor(ctx context.Context, clientID string) int64 {
	resp, err := s.Cluster.MixCoordClient.GetClientTelemetry(ctx, &milvuspb.GetClientTelemetryRequest{
		ClientId:       clientID,
		IncludeMetrics: true,
	})
	if err != nil || len(resp.GetClients()) == 0 {
		return 0
	}
	for _, op := range resp.GetClients()[0].GetMetrics() {
		if op.GetOperation() == "Search" {
			return op.GetGlobal().GetRequestCount()
		}
	}
	return 0
}

// TestMetricsReachTheCoordinator covers the upward half of the round trip: operations the
// SDK performed show up in a telemetry query, attributed to that client.
func (s *TelemetrySuite) TestMetricsReachTheCoordinator() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clientID := s.sdk.GetTelemetry().GetClientID()
	s.Require().NotEmpty(clientID)

	var searchRequests int64
	s.pollWhileSearching(ctx, "Search metrics for this client", func() bool {
		searchRequests = s.searchRequestsFor(ctx, clientID)
		return searchRequests > 0
	})

	s.Positive(searchRequests, "the coordinator reported a Search window with no requests in it")
}

// TestPushedCommandReachesTheClient covers the downward half: a command pushed at the
// coordinator is delivered on a heartbeat and executed by the SDK's handler.
func (s *TelemetrySuite) TestPushedCommandReachesTheClient() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// A handler for a type the SDK does not know about, so what it observes can only have
	// come from this push.
	received := make(chan string, 1)
	s.sdk.GetTelemetry().RegisterCommandHandler("integration_probe", func(cmd *milvusclient.ClientCommand) *milvusclient.CommandReply {
		select {
		case received <- string(cmd.Payload):
		default:
		}
		return &milvusclient.CommandReply{CommandId: cmd.CommandId, Success: true}
	})

	resp, err := s.Cluster.MixCoordClient.PushClientCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "integration_probe",
		TargetClientId: s.sdk.GetTelemetry().GetClientID(),
		Payload:        []byte(`{"probe":true}`),
		TtlSeconds:     300,
		Persistent:     false,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	s.Require().NotEmpty(resp.GetCommandId())

	select {
	case payload := <-received:
		s.Equal(`{"probe":true}`, payload)
	case <-time.After(pollTimeout):
		s.FailNow("pushed command never reached the client")
	}
}

// TestOnlyPushConfigMayBePersistent pins the rule that decides whether a command becomes a
// durable config in etcd or a one-time delivery. It is asserted here rather than only in the
// coordinator's unit tests because it is enforced across an RPC boundary, and an SDK-side
// example got this wrong for the entire life of the feature without anything noticing.
func (s *TelemetrySuite) TestOnlyPushConfigMayBePersistent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	resp, err := s.Cluster.MixCoordClient.PushClientCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "collection_metrics",
		Payload:     []byte(`{"enabled":true}`),
		Persistent:  true,
	})
	err = merr.CheckRPCCall(resp.GetStatus(), err)
	s.Require().Error(err)
	// Asserted on the message rather than with errors.Is: a merr error crossing the
	// coordinator's gRPC boundary arrives as code = Unknown with everything flattened into
	// the description, so the ErrParameterInvalid sentinel is no longer in the chain. The
	// wording is the part a caller can actually act on, so it is the part pinned here.
	s.ErrorContains(err, "only push_config can be persistent")

	// The same command is accepted as a one-time delivery, so the rejection is about
	// persistence and not about the command type being unsupported.
	resp, err = s.Cluster.MixCoordClient.PushClientCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "collection_metrics",
		Payload:     []byte(`{"enabled":true}`),
		TtlSeconds:  300,
		Persistent:  false,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	s.NotEmpty(resp.GetCommandId())

	// push_config is the one type that may persist, and a global scope avoids the separate
	// rule that a client-scoped persistent config needs a stable client ID.
	resp, err = s.Cluster.MixCoordClient.PushClientCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType: "push_config",
		Payload:     []byte(`{"sampling_rate":0.5}`),
		Persistent:  true,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))

	deleteResp, err := s.Cluster.MixCoordClient.DeleteClientCommand(ctx, &milvuspb.DeleteClientCommandRequest{
		CommandId: resp.GetCommandId(),
	})
	s.Require().NoError(merr.CheckRPCCall(deleteResp.GetStatus(), err))
}

// TestPersistentConfigNeedsAStableClientID pins the other half of the persistence rule: a
// config aimed at a client whose ID is regenerated on restart would stop matching, silently,
// while remaining in etcd -- so it is refused rather than accepted and quietly ineffective.
func (s *TelemetrySuite) TestPersistentConfigNeedsAStableClientID() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clientID := s.sdk.GetTelemetry().GetClientID()

	// The check reports "not ready" for a client the coordinator has not seen yet, which is
	// retryable and not the rejection under test, so wait until it is known.
	s.pollWhileSearching(ctx, "the client to become known to the coordinator", func() bool {
		resp, err := s.Cluster.MixCoordClient.GetClientTelemetry(ctx, &milvuspb.GetClientTelemetryRequest{ClientId: clientID})
		return err == nil && len(resp.GetClients()) > 0
	})

	resp, err := s.Cluster.MixCoordClient.PushClientCommand(ctx, &milvuspb.PushClientCommandRequest{
		CommandType:    "push_config",
		TargetClientId: clientID,
		Payload:        []byte(`{"sampling_rate":0.5}`),
		Persistent:     true,
	})
	err = merr.CheckRPCCall(resp.GetStatus(), err)
	s.Require().Error(err)
	// See the note in TestOnlyPushConfigMayBePersistent on why this matches text rather
	// than the sentinel. The reason is asserted too: refusing for the wrong reason -- a
	// cold coordinator cache, say -- would pass a bare "is an error" check.
	s.ErrorContains(err, "generated client ID")
}

// TestQuietIntervalDoesNotBlankTheView covers what the retained windows are for. A client
// that pauses for longer than its heartbeat interval reports an empty window, and if that
// were the only window kept, a connected client that was busy a moment ago would read as one
// that does nothing at all.
//
// It also exercises the config path end to end: retainedWindows arrives through
// rootCoord.clientTelemetry.retainedWindows, so the assertion below only holds if the param
// actually reached the coordinator.
func (s *TelemetrySuite) TestQuietIntervalDoesNotBlankTheView() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clientID := s.sdk.GetTelemetry().GetClientID()

	// Get traffic into the reported window first; without this the assertion could pass on
	// a client that never did anything.
	s.pollWhileSearching(ctx, "Search metrics before going quiet", func() bool {
		return s.searchRequestsFor(ctx, clientID) > 0
	})

	// Then stop entirely. Two heartbeats without traffic is more than enough to blank the
	// view if only the newest window were kept, and far short of the retainedWindows-1
	// intervals it takes for the last busy window to age out.
	time.Sleep(2*heartbeat + heartbeat/2)

	s.Positive(s.searchRequestsFor(ctx, clientID),
		"an idle client that is still connected reported no metrics at all; "+
			"the busy window aged out earlier than %d retained windows allows", retainedWindows)
}

func TestTelemetry(t *testing.T) {
	suite.Run(t, new(TelemetrySuite))
}
