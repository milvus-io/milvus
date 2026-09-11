package observe

import (
	"context"
	"testing"

	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestConcreteEventsImplementEvent(t *testing.T) {
	var _ Event = CoordQueryNodeLostDetectedEvent{}
	var _ Event = CoordViewCreatedEvent{}
	var _ Event = CoordViewPreemptedEvent{}
	var _ Event = CoordViewAdvancedFromUnrecoverableEvent{}
	var _ Event = CoordViewReleaseRequestedEvent{}
	var _ Event = CoordViewHandoffToNewUpEvent{}
	var _ Event = CoordViewReportAppliedEvent{}
	var _ Event = CoordViewQueryNodeLostAppliedEvent{}
	var _ Event = QueryNodeApplyCoordViewEvent{}
	var _ Event = QueryNodeSegmentUnrecoverableEvent{}
	var _ Event = QueryNodeReportViewEvent{}
	var _ Event = QueryNodeReleaseDoneEvent{}
	var _ Event = StreamingNodeApplyCoordViewEvent{}
	var _ Event = StreamingNodeRecoveringDoneEvent{}
	var _ Event = StreamingNodeReportViewEvent{}
	var _ Event = StreamingNodeReleaseDoneEvent{}
	var _ Event = QueryNodeAcquireSegmentsEvent{}
	var _ Event = QueryNodeSegmentsReadyEvent{}
	var _ Event = QueryNodeReleaseSegmentsEvent{}
	var _ Event = StreamingNodeAcquireResourceEvent{}
	var _ Event = StreamingNodeRecoverAcquireResourceEvent{}
	var _ Event = StreamingNodeResourceReadyEvent{}
	var _ Event = CoordPersistViewEvent{}
	var _ Event = StreamingNodePersistViewEvent{}
	var _ Event = CoordSyncViewAcceptedEvent{}
}

func TestLogObserverImplementsObserver(t *testing.T) {
	var _ Observer = LogObserver{}
}

func TestDefaultRegistryIncludesLogObserver(t *testing.T) {
	defaultRegistry.mu.RLock()
	defer defaultRegistry.mu.RUnlock()

	if len(defaultRegistry.observers) == 0 {
		t.Fatal("default registry has no observers")
	}
	if _, ok := defaultRegistry.observers[0].(LogObserver); !ok {
		t.Fatalf("default registry first observer = %T, want LogObserver", defaultRegistry.observers[0])
	}
}

func TestRegistryObserveFanoutsToRegisteredObservers(t *testing.T) {
	first := &recordingObserver{}
	second := &recordingObserver{}
	registry := NewRegistry(first)
	registry.Register(second)
	event := CoordViewCreatedEvent{
		View:  testQueryViewKey(),
		State: qviews.QueryViewStatePreparing,
	}

	registry.Observe(context.Background(), event)

	if len(first.events) != 1 {
		t.Fatalf("first observer events = %d, want 1", len(first.events))
	}
	if len(second.events) != 1 {
		t.Fatalf("second observer events = %d, want 1", len(second.events))
	}
	if first.events[0] != event {
		t.Fatalf("first observer event = %#v, want %#v", first.events[0], event)
	}
	if second.events[0] != event {
		t.Fatalf("second observer event = %#v, want %#v", second.events[0], event)
	}
}

type recordingObserver struct {
	events []Event
}

func (o *recordingObserver) Observe(_ context.Context, event Event) {
	o.events = append(o.events, event)
}

func TestPrivateMarkerStructProvidesMarker(t *testing.T) {
	var _ interface{ isQueryViewEvent() } = baseEvent{}
}

func TestEventMarshalLogObjectSplitsQueryViewKeyAndUsesStringers(t *testing.T) {
	view := testQueryViewKey()
	event := CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStatePreparing,
			To:   qviews.QueryViewStateReady,
		},
		Node:          qviews.NewQueryNode(10),
		ReportedState: qviews.QueryViewStateReady,
	}
	enc := zapcore.NewMapObjectEncoder()

	if err := event.MarshalLogObject(enc); err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	assertField(t, enc, "type", "CoordViewReportApplied")
	assertField(t, enc, "sid", view.ShardID.String())
	assertField(t, enc, "qv", view.QueryViewVersion.String())
	assertField(t, enc, "dv", view.QueryViewVersion.DataVersion.String())
	assertField(t, enc, "state", qviews.QueryViewStatePreparing.String()+"->"+qviews.QueryViewStateReady.String())
	assertField(t, enc, "wn", qviews.NewQueryNode(10).String())
	assertField(t, enc, "reportedState", qviews.QueryViewStateReady.String())
}

func TestEventMarshalLogObjectSplitsAdditionalQueryViewKey(t *testing.T) {
	view := testQueryViewKey()
	newUpView := qviews.QueryViewKey{
		ShardID: qviews.ShardID{
			ReplicaID: 2,
			VChannel:  "v2",
		},
		QueryViewVersion: qviews.QueryViewVersion{
			DataVersion: qviews.DataVersion{
				StreamingVersion: 30,
				CompactVersion:   40,
			},
			QueryVersion: 5,
		},
	}
	event := CoordViewHandoffToNewUpEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStateUp,
			To:   qviews.QueryViewStateDown,
		},
		NewUpView: newUpView,
	}
	enc := zapcore.NewMapObjectEncoder()

	if err := event.MarshalLogObject(enc); err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	assertField(t, enc, "sid", view.ShardID.String())
	assertField(t, enc, "qv", view.QueryViewVersion.String())
	assertField(t, enc, "dv", view.QueryViewVersion.DataVersion.String())
	assertField(t, enc, "newUpSid", newUpView.ShardID.String())
	assertField(t, enc, "newUpQv", newUpView.QueryViewVersion.String())
	assertField(t, enc, "newUpDv", newUpView.QueryViewVersion.DataVersion.String())
}

func TestFieldEventWrapsEventAsMlogObject(t *testing.T) {
	field := FieldEvent(CoordViewCreatedEvent{
		View:  testQueryViewKey(),
		State: qviews.QueryViewStatePreparing,
	})
	enc := zapcore.NewMapObjectEncoder()

	field.AddTo(enc)

	if field.Key != "" {
		t.Fatalf("unexpected inline field key: %s", field.Key)
	}
	if field.Type != zapcore.InlineMarshalerType {
		t.Fatalf("unexpected field type: %v", field.Type)
	}
	assertField(t, enc, "type", "CoordViewCreated")
	assertField(t, enc, "state", qviews.QueryViewStatePreparing.String())
}

func TestEventTypeShortensNodeNamesAndDropsEventSuffix(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected string
	}{
		{
			name: "query node",
			event: QueryNodeReportViewEvent{
				View:  testQueryViewKey(),
				State: qviews.QueryViewStateReady,
			},
			expected: "QNReportView",
		},
		{
			name: "streaming node",
			event: StreamingNodeReportViewEvent{
				View:  testQueryViewKey(),
				State: qviews.QueryViewStateUp,
			},
			expected: "SNReportView",
		},
		{
			name: "query node in coord event",
			event: CoordViewQueryNodeLostAppliedEvent{
				ViewStateTransition: ViewStateTransition{
					View: testQueryViewKey(),
					From: qviews.QueryViewStateUp,
					To:   qviews.QueryViewStateUnrecoverable,
				},
				Node: qviews.NewQueryNode(10),
			},
			expected: "CoordViewQNLostApplied",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc := zapcore.NewMapObjectEncoder()

			if err := test.event.MarshalLogObject(enc); err != nil {
				t.Fatalf("marshal event: %v", err)
			}

			assertField(t, enc, "type", test.expected)
		})
	}
}

func TestEventLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		event Event
		level mlog.Level
	}{
		{
			name:  "normal event",
			event: CoordViewCreatedEvent{},
			level: mlog.InfoLevel,
		},
		{
			name:  "querynode lost event",
			event: CoordQueryNodeLostDetectedEvent{},
			level: mlog.DebugLevel,
		},
		{
			name:  "segment unrecoverable event",
			event: QueryNodeSegmentUnrecoverableEvent{},
			level: mlog.WarnLevel,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.event.LogLevel(); got != test.level {
				t.Fatalf("LogLevel() = %v, want %v", got, test.level)
			}
		})
	}
}

func TestEventTriggerInfo(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected string
	}{
		{
			name: "coord report ready",
			event: CoordViewReportAppliedEvent{
				ViewStateTransition: ViewStateTransition{
					View: testQueryViewKey(),
					From: qviews.QueryViewStatePreparing,
					To:   qviews.QueryViewStateReady,
				},
				ReportedState: qviews.QueryViewStateReady,
			},
			expected: "reportReady",
		},
		{
			name: "coord report unrecoverable",
			event: CoordViewReportAppliedEvent{
				ViewStateTransition: ViewStateTransition{
					View: testQueryViewKey(),
					From: qviews.QueryViewStatePreparing,
					To:   qviews.QueryViewStateUnrecoverable,
				},
				ReportedState: qviews.QueryViewStateUnrecoverable,
			},
			expected: "reportUnrecoverable",
		},
		{
			name: "preempt",
			event: CoordViewPreemptedEvent{
				ViewStateTransition: ViewStateTransition{
					View: testQueryViewKey(),
					From: qviews.QueryViewStatePreparing,
					To:   qviews.QueryViewStateUnrecoverable,
				},
			},
			expected: "preempt",
		},
		{
			name: "event without transition trigger",
			event: CoordViewCreatedEvent{
				View:  testQueryViewKey(),
				State: qviews.QueryViewStatePreparing,
			},
			expected: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.event.TriggerInfo(); got != test.expected {
				t.Fatalf("TriggerInfo() = %q, want %q", got, test.expected)
			}
		})
	}
}

func TestEventComponentInfo(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected string
	}{
		{
			name: "coord event",
			event: CoordViewReportAppliedEvent{
				ViewStateTransition: ViewStateTransition{
					View: testQueryViewKey(),
					From: qviews.QueryViewStatePreparing,
					To:   qviews.QueryViewStateReady,
				},
				ReportedState: qviews.QueryViewStateReady,
			},
			expected: "coord",
		},
		{
			name: "query node event",
			event: QueryNodeSegmentsReadyEvent{
				ViewStateTransition: ViewStateTransition{
					View: testQueryViewKey(),
					From: qviews.QueryViewStatePreparing,
					To:   qviews.QueryViewStateReady,
				},
			},
			expected: "queryNode",
		},
		{
			name: "streaming node event",
			event: StreamingNodeResourceReadyEvent{
				ViewStateTransition: ViewStateTransition{
					View: testQueryViewKey(),
					From: qviews.QueryViewStatePreparing,
					To:   qviews.QueryViewStateReady,
				},
			},
			expected: "streamingNode",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.event.ComponentInfo(); got != test.expected {
				t.Fatalf("ComponentInfo() = %q, want %q", got, test.expected)
			}
		})
	}
}

func TestCoordSyncViewAcceptedEventMarshalLogObject(t *testing.T) {
	view := testQueryViewKey()
	node := qviews.NewQueryNode(10)
	event := CoordSyncViewAcceptedEvent{
		View:  view,
		Node:  node,
		State: qviews.QueryViewStatePreparing,
	}
	enc := zapcore.NewMapObjectEncoder()

	if err := event.MarshalLogObject(enc); err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	assertField(t, enc, "type", "CoordSyncViewAccepted")
	assertField(t, enc, "sid", view.ShardID.String())
	assertField(t, enc, "qv", view.QueryViewVersion.String())
	assertField(t, enc, "dv", view.QueryViewVersion.DataVersion.String())
	assertField(t, enc, "wn", node.String())
	assertField(t, enc, "state", qviews.QueryViewStatePreparing.String())
}

func testQueryViewKey() qviews.QueryViewKey {
	return qviews.QueryViewKey{
		ShardID: qviews.ShardID{
			ReplicaID: 1,
			VChannel:  "v1",
		},
		QueryViewVersion: qviews.QueryViewVersion{
			DataVersion: qviews.DataVersion{
				StreamingVersion: 10,
				CompactVersion:   20,
			},
			QueryVersion: 3,
		},
	}
}

func assertField(t *testing.T, enc *zapcore.MapObjectEncoder, key string, expected any) {
	t.Helper()
	actual, ok := enc.Fields[key]
	if !ok {
		t.Fatalf("missing field %q", key)
	}
	if actual != expected {
		t.Fatalf("field %q = %v, want %v", key, actual, expected)
	}
}
