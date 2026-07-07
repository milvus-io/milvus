package observe

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	fieldType                 = "type"
	fieldSID                  = "sid"
	fieldQV                   = "qv"
	fieldDV                   = "dv"
	fieldState                = "state"
	fieldPreemptingDV         = "preemptingDv"
	fieldNewUpSID             = "newUpSid"
	fieldNewUpQV              = "newUpQv"
	fieldNewUpDV              = "newUpDv"
	fieldWN                   = "wn"
	fieldReportedState        = "reportedState"
	fieldResourceReadyPercent = "resourceReadyPercent"
	fieldError                = "error"
	fieldSegmentID            = "segmentID"
	fieldSegmentCount         = "segmentCount"
	fieldReadySegmentCount    = "readySegmentCount"
)

const (
	eventCoordQNLostDetected                = "CoordQNLostDetected"
	eventCoordViewCreated                   = "CoordViewCreated"
	eventCoordViewPreempted                 = "CoordViewPreempted"
	eventCoordViewAdvancedFromUnrecoverable = "CoordViewAdvancedFromUnrecoverable"
	eventCoordViewReleaseRequested          = "CoordViewReleaseRequested"
	eventCoordViewHandoffToNewUp            = "CoordViewHandoffToNewUp"
	eventCoordViewReportApplied             = "CoordViewReportApplied"
	eventCoordViewQNLostApplied             = "CoordViewQNLostApplied"
	eventQNApplyCoordView                   = "QNApplyCoordView"
	eventQNSegmentUnrecoverable             = "QNSegmentUnrecoverable"
	eventQNReportView                       = "QNReportView"
	eventQNReleaseDone                      = "QNReleaseDone"
	eventSNApplyCoordView                   = "SNApplyCoordView"
	eventSNRecoveringDone                   = "SNRecoveringDone"
	eventSNReportView                       = "SNReportView"
	eventSNReleaseDone                      = "SNReleaseDone"
	eventQNSegmentFailure                   = "QNSegmentFailure"
	eventQNAcquireSegments                  = "QNAcquireSegments"
	eventQNSegmentsReady                    = "QNSegmentsReady"
	eventQNReleaseSegments                  = "QNReleaseSegments"
	eventSNAcquireResource                  = "SNAcquireResource"
	eventSNRecoverAcquireResource           = "SNRecoverAcquireResource"
	eventSNResourceReady                    = "SNResourceReady"
	eventSNReleaseResource                  = "SNReleaseResource"
	eventCoordPersistView                   = "CoordPersistView"
	eventSNPersistView                      = "SNPersistView"
	eventCoordSyncViewBatch                 = "CoordSyncViewBatch"
	eventCoordSyncViewBatchFailed           = "CoordSyncViewBatchFailed"
)

// Event is the sealed interface implemented by all QueryView observability events.
type Event interface {
	mlog.ObjectMarshaler
	LogLevel() mlog.Level
	isQueryViewEvent()
}

// baseEvent provides the private marker method for concrete events.
type baseEvent struct{}

func (baseEvent) isQueryViewEvent() {}

// FieldEvent returns an inline mlog field for a QueryView event.
func FieldEvent(event Event) mlog.Field {
	return mlog.Inline(event)
}

// ViewStateTransition identifies one QueryView state transition.
type ViewStateTransition struct {
	View qviews.QueryViewKey
	From qviews.QueryViewState
	To   qviews.QueryViewState
}

// CoordQueryNodeLostDetectedEvent is emitted when Coord sync code observes
// QueryNode loss.
type CoordQueryNodeLostDetectedEvent struct {
	baseEvent
	Node qviews.QueryNode
}

func (e CoordQueryNodeLostDetectedEvent) LogLevel() mlog.Level {
	return mlog.WarnLevel
}

func (e CoordQueryNodeLostDetectedEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordQNLostDetected)
	enc.AddString(fieldWN, e.Node.String())
	return nil
}

// CoordViewCreatedEvent is emitted after Coord creates a new Preparing view.
type CoordViewCreatedEvent struct {
	baseEvent
	View  qviews.QueryViewKey
	State qviews.QueryViewState
}

func (e CoordViewCreatedEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e CoordViewCreatedEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordViewCreated)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.State.String())
	return nil
}

// CoordViewPreemptedEvent is emitted after Coord preempts a Preparing or Ready
// view while adding a new Preparing view.
type CoordViewPreemptedEvent struct {
	baseEvent
	ViewStateTransition
	PreemptingDataVersion qviews.DataVersion
}

func (e CoordViewPreemptedEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e CoordViewPreemptedEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordViewPreempted)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	enc.AddString(fieldPreemptingDV, e.PreemptingDataVersion.String())
	return nil
}

// CoordViewAdvancedFromUnrecoverableEvent is emitted after Coord advances an
// Unrecoverable view to Dropping.
type CoordViewAdvancedFromUnrecoverableEvent struct {
	baseEvent
	ViewStateTransition
}

func (e CoordViewAdvancedFromUnrecoverableEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e CoordViewAdvancedFromUnrecoverableEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordViewAdvancedFromUnrecoverable)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	return nil
}

// CoordViewReleaseRequestedEvent is emitted after ShardViewManager applies
// RequestRelease to a view.
type CoordViewReleaseRequestedEvent struct {
	baseEvent
	ViewStateTransition
}

func (e CoordViewReleaseRequestedEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e CoordViewReleaseRequestedEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordViewReleaseRequested)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	return nil
}

// CoordViewHandoffToNewUpEvent is emitted after ShardViewManager transitions
// the previous Up view to Down because another view became Up.
type CoordViewHandoffToNewUpEvent struct {
	baseEvent
	ViewStateTransition
	NewUpView qviews.QueryViewKey
}

func (e CoordViewHandoffToNewUpEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e CoordViewHandoffToNewUpEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordViewHandoffToNewUp)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	enc.AddString(fieldNewUpSID, e.NewUpView.ShardID.String())
	enc.AddString(fieldNewUpQV, e.NewUpView.QueryViewVersion.String())
	enc.AddString(fieldNewUpDV, e.NewUpView.QueryViewVersion.DataVersion.String())
	return nil
}

// CoordViewReportAppliedEvent is emitted after ShardViewManager applies a
// work-node report to a view. ResourceReadyPercent is the report-side resource
// preparation progress in [0, 100]. StreamingNode reports derive this value
// from view state: resource-ready states report 100, other states report 0.
type CoordViewReportAppliedEvent struct {
	baseEvent
	ViewStateTransition
	Node                 qviews.WorkNode
	ReportedState        qviews.QueryViewState
	ResourceReadyPercent int64
}

func (e CoordViewReportAppliedEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e CoordViewReportAppliedEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordViewReportApplied)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	if e.Node != nil {
		enc.AddString(fieldWN, e.Node.String())
	}
	enc.AddString(fieldReportedState, e.ReportedState.String())
	enc.AddInt64(fieldResourceReadyPercent, e.ResourceReadyPercent)
	return nil
}

// CoordViewQueryNodeLostAppliedEvent is emitted after ShardViewManager applies
// QueryNode loss to a view.
type CoordViewQueryNodeLostAppliedEvent struct {
	baseEvent
	ViewStateTransition
	Node qviews.QueryNode
}

func (e CoordViewQueryNodeLostAppliedEvent) LogLevel() mlog.Level {
	return mlog.WarnLevel
}

func (e CoordViewQueryNodeLostAppliedEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordViewQNLostApplied)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	enc.AddString(fieldWN, e.Node.String())
	return nil
}

// QueryNodeApplyCoordViewEvent is emitted after QueryNode applies a Coord view
// state.
type QueryNodeApplyCoordViewEvent struct {
	baseEvent
	ViewStateTransition
}

func (e QueryNodeApplyCoordViewEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e QueryNodeApplyCoordViewEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventQNApplyCoordView)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	return nil
}

// QueryNodeSegmentUnrecoverableEvent is emitted after the segment
// unrecoverable callback moves a view to Unrecoverable.
type QueryNodeSegmentUnrecoverableEvent struct {
	baseEvent
	ViewStateTransition
	Err error
}

func (e QueryNodeSegmentUnrecoverableEvent) LogLevel() mlog.Level {
	return mlog.WarnLevel
}

func (e QueryNodeSegmentUnrecoverableEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventQNSegmentUnrecoverable)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	if e.Err != nil {
		enc.AddString(fieldError, e.Err.Error())
	}
	return nil
}

// QueryNodeReportViewEvent is emitted when QueryNode reports local view state.
type QueryNodeReportViewEvent struct {
	baseEvent
	View  qviews.QueryViewKey
	State qviews.QueryViewState
}

func (e QueryNodeReportViewEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e QueryNodeReportViewEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventQNReportView)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.State.String())
	return nil
}

// QueryNodeReleaseDoneEvent is emitted after QueryNode observes release
// completion for a view.
type QueryNodeReleaseDoneEvent struct {
	baseEvent
	ViewStateTransition
}

func (e QueryNodeReleaseDoneEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e QueryNodeReleaseDoneEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventQNReleaseDone)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	return nil
}

// StreamingNodeApplyCoordViewEvent is emitted after StreamingNode applies a
// Coord view state.
type StreamingNodeApplyCoordViewEvent struct {
	baseEvent
	ViewStateTransition
}

func (e StreamingNodeApplyCoordViewEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodeApplyCoordViewEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNApplyCoordView)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	return nil
}

// StreamingNodeRecoveringDoneEvent is emitted after StreamingNode observes
// recovery completion for a view.
type StreamingNodeRecoveringDoneEvent struct {
	baseEvent
	ViewStateTransition
}

func (e StreamingNodeRecoveringDoneEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodeRecoveringDoneEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNRecoveringDone)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	return nil
}

// StreamingNodeReportViewEvent is emitted when StreamingNode reports local view
// state.
type StreamingNodeReportViewEvent struct {
	baseEvent
	View  qviews.QueryViewKey
	State qviews.QueryViewState
}

func (e StreamingNodeReportViewEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodeReportViewEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNReportView)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.State.String())
	return nil
}

// StreamingNodeReleaseDoneEvent is emitted after StreamingNode observes release
// completion for a view.
type StreamingNodeReleaseDoneEvent struct {
	baseEvent
	ViewStateTransition
}

func (e StreamingNodeReleaseDoneEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodeReleaseDoneEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNReleaseDone)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	return nil
}

// QueryNodeSegmentFailureEvent is emitted when physical segment load or
// transform-log catch-up fails.
type QueryNodeSegmentFailureEvent struct {
	baseEvent
	View      qviews.QueryViewKey
	SegmentID int64
	Err       error
}

func (e QueryNodeSegmentFailureEvent) LogLevel() mlog.Level {
	return mlog.WarnLevel
}

func (e QueryNodeSegmentFailureEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventQNSegmentFailure)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddInt64(fieldSegmentID, e.SegmentID)
	if e.Err != nil {
		enc.AddString(fieldError, e.Err.Error())
	}
	return nil
}

// QueryNodeAcquireSegmentsEvent is emitted when QueryNode starts acquiring
// segments for a new Preparing view.
type QueryNodeAcquireSegmentsEvent struct {
	baseEvent
	View         qviews.QueryViewKey
	SegmentCount int
}

func (e QueryNodeAcquireSegmentsEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e QueryNodeAcquireSegmentsEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventQNAcquireSegments)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddInt(fieldSegmentCount, e.SegmentCount)
	return nil
}

// QueryNodeSegmentsReadyEvent is emitted after the segment readiness callback
// moves a view forward.
type QueryNodeSegmentsReadyEvent struct {
	baseEvent
	ViewStateTransition
	ReadySegmentCount int
}

func (e QueryNodeSegmentsReadyEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e QueryNodeSegmentsReadyEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventQNSegmentsReady)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	enc.AddInt(fieldReadySegmentCount, e.ReadySegmentCount)
	return nil
}

// QueryNodeReleaseSegmentsEvent is emitted when QueryNode starts releasing
// segments for a view.
type QueryNodeReleaseSegmentsEvent struct {
	baseEvent
	View qviews.QueryViewKey
}

func (e QueryNodeReleaseSegmentsEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e QueryNodeReleaseSegmentsEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventQNReleaseSegments)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	return nil
}

// StreamingNodeAcquireResourceEvent is emitted when StreamingNode starts
// acquiring resources for a new Preparing view.
type StreamingNodeAcquireResourceEvent struct {
	baseEvent
	View qviews.QueryViewKey
}

func (e StreamingNodeAcquireResourceEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodeAcquireResourceEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNAcquireResource)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	return nil
}

// StreamingNodeRecoverAcquireResourceEvent is emitted when StreamingNode starts
// acquiring resources for a recovered Up view.
type StreamingNodeRecoverAcquireResourceEvent struct {
	baseEvent
	View qviews.QueryViewKey
}

func (e StreamingNodeRecoverAcquireResourceEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodeRecoverAcquireResourceEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNRecoverAcquireResource)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	return nil
}

// StreamingNodeResourceReadyEvent is emitted after the resource ready callback
// moves a view forward.
type StreamingNodeResourceReadyEvent struct {
	baseEvent
	ViewStateTransition
}

func (e StreamingNodeResourceReadyEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodeResourceReadyEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNResourceReady)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.From.String()+"->"+e.To.String())
	return nil
}

// StreamingNodeReleaseResourceEvent is emitted when StreamingNode starts
// releasing resources for a view.
type StreamingNodeReleaseResourceEvent struct {
	baseEvent
	View qviews.QueryViewKey
}

func (e StreamingNodeReleaseResourceEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodeReleaseResourceEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNReleaseResource)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	return nil
}

// CoordPersistViewEvent is emitted when ShardViewManager.flush persists a view
// state.
type CoordPersistViewEvent struct {
	baseEvent
	View  qviews.QueryViewKey
	State qviews.QueryViewState
}

func (e CoordPersistViewEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e CoordPersistViewEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordPersistView)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.State.String())
	return nil
}

// StreamingNodePersistViewEvent is emitted when StreamingNode persists local
// view state.
type StreamingNodePersistViewEvent struct {
	baseEvent
	View  qviews.QueryViewKey
	State qviews.QueryViewState
}

func (e StreamingNodePersistViewEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e StreamingNodePersistViewEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventSNPersistView)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.State.String())
	return nil
}

// CoordSyncViewBatchEvent is emitted when ShardViewManager.flush syncs a view
// state to one worker-node batch.
type CoordSyncViewBatchEvent struct {
	baseEvent
	View  qviews.QueryViewKey
	State qviews.QueryViewState
}

func (e CoordSyncViewBatchEvent) LogLevel() mlog.Level {
	return mlog.InfoLevel
}

func (e CoordSyncViewBatchEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordSyncViewBatch)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.State.String())
	return nil
}

// CoordSyncViewBatchFailedEvent is emitted when ShardViewManager.flush fails to
// sync a view state to one worker-node batch.
type CoordSyncViewBatchFailedEvent struct {
	baseEvent
	View  qviews.QueryViewKey
	State qviews.QueryViewState
	Err   error
}

func (e CoordSyncViewBatchFailedEvent) LogLevel() mlog.Level {
	return mlog.WarnLevel
}

func (e CoordSyncViewBatchFailedEvent) MarshalLogObject(enc mlog.ObjectEncoder) error {
	enc.AddString(fieldType, eventCoordSyncViewBatchFailed)
	enc.AddString(fieldSID, e.View.ShardID.String())
	enc.AddString(fieldQV, e.View.QueryViewVersion.String())
	enc.AddString(fieldDV, e.View.QueryViewVersion.DataVersion.String())
	enc.AddString(fieldState, e.State.String())
	if e.Err != nil {
		enc.AddString(fieldError, e.Err.Error())
	}
	return nil
}
