package datacoord

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ CompactionTask = (*mixCompactionTask)(nil)

type mixCompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask

	allocator allocator.Allocator
	meta      CompactionMeta

	ievm IndexEngineVersionManager

	times *taskcommon.Times

	slotUsage atomic.Int64
	// requirement caches the two-dimensional estimate the plan ships. The
	// scalar above stays as its fold, for a worker that predates the vector.
	requirement atomic.Pointer[taskresource.Requirement]
}

func (t *mixCompactionTask) GetTaskID() int64 {
	return t.GetTaskProto().GetPlanID()
}

func (t *mixCompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *mixCompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTaskProto().GetState())
}

// price resolves the input segments ONCE and derives both currencies from that
// single walk: the scalar an un-upgraded worker accounts in, and the vector a
// current coordinator places on. Two independent walks would double the meta
// lookups on the scheduler's per-round path, and could disagree about which
// segments still exist.
func (t *mixCompactionTask) price() (int64, taskresource.Requirement) {
	if slot, req := t.slotUsage.Load(), t.requirement.Load(); slot != 0 && req != nil {
		return slot, *req
	}
	segments, allResolved := t.resolveInputSegments(context.TODO())
	return t.legacyTaskSlot(segments), t.computeAndCacheRequirement(segments, allResolved)
}

// GetTaskSlot is the scalar an un-upgraded worker reads, and it deliberately
// reports the LEGACY figure -- the flat constant for mix, the segment-size step
// function for sort.
//
// The estimate does not go here. Folding a byte estimate onto this field would
// keep its name and change its scale, and a worker that has not restarted would
// read the new number against the old scale: a 4.5GiB storage-v3 compaction
// moves from 4 slots to about 36, so a new coordinator reads such a worker as
// full roughly nine times too early, on every compaction. That is a
// cluster-wide throughput collapse reachable by an ordinary partial rollout,
// and it is what an earlier kill switch existed to undo. Sending each worker
// the currency it understands -- the vector to those that understand it, the
// old constants to those that do not -- removes the collision instead of
// providing a lever for it.
func (t *mixCompactionTask) GetTaskSlot() int64 {
	slot, _ := t.price()
	return slot
}

// TaskResources states this task's requirement to the scheduler BEFORE it is
// dispatched, which is what lets the node be chosen for having room rather
// than for a scalar that stands in for it.
func (t *mixCompactionTask) TaskResources() *datapb.TaskResources {
	_, req := t.price()
	return req.ToProto()
}

// taskRequirement is the two-dimensional estimate that goes on the wire.
func (t *mixCompactionTask) taskRequirement() taskresource.Requirement {
	_, req := t.price()
	return req
}

// resolveInputSegments fetches every input segment via meta.GetHealthySegment.
// It returns the segments that resolved and whether every input segment did;
// a segment that fails to resolve is logged by ID rather than silently
// dropped, since it otherwise under-charges the estimate with no signal for
// an operator to go on.
func (t *mixCompactionTask) resolveInputSegments(ctx context.Context) ([]*SegmentInfo, bool) {
	inputSegments := t.GetTaskProto().GetInputSegments()
	segments := make([]*SegmentInfo, 0, len(inputSegments))
	allResolved := true
	for _, segID := range inputSegments {
		segment := t.meta.GetHealthySegment(ctx, segID)
		if segment == nil {
			allResolved = false
			mlog.Warn(ctx, "mixCompactionTask could not resolve input segment for slot estimation, estimate will under-count it",
				mlog.Int64("planID", t.GetTaskID()), mlog.Int64("segmentID", segID))
			continue
		}
		segments = append(segments, segment)
	}
	return segments, allResolved
}

// computeAndCacheTaskSlot derives the slot usage from already-resolved
// segments, so callers that already have the segments in hand (i.e.
// BuildCompactionRequest) don't pay for a second meta fetch -- and don't
// race it, since the plan's own SlotUsage would then quietly disagree with
// which segments the plan actually ships.
//
// allResolved must be true only when every one of the task's input segments
// (not just the ones passed in) resolved: a partial estimate is a real,
// immediately-usable number, but caching it would turn a transient
// resolution failure into a permanently wrong slot count for this task
// instance, since GetTaskSlot short-circuits once t.slotUsage is non-zero.
func (t *mixCompactionTask) computeAndCacheRequirement(segments []*SegmentInfo, allResolved bool) taskresource.Requirement {
	if cached := t.requirement.Load(); cached != nil {
		return *cached
	}

	req := compactionRequirement(t.GetTaskProto().GetType(), segments, t.GetTaskProto().GetSchema())
	// Cache only a complete estimate: a partial one is immediately usable but
	// caching it would turn a transient resolution failure into a permanently
	// wrong figure for this task instance.
	if allResolved {
		t.requirement.Store(&req)
	}
	mlog.Info(context.TODO(), "mixCompactionTask priced task",
		mlog.Int64("planID", t.GetTaskID()),
		mlog.String("requirement", req.String()),
		mlog.Bool("cached", allResolved))
	return req
}

// legacyTaskSlot is the pricing this task has always reported on the scalar
// field: a flat constant for mix, the segment-size step function for sort. It
// is not a fallback -- it is what the scalar field MEANS, and it keeps meaning
// that so an un-upgraded worker's own accounting stays self-consistent.
func (t *mixCompactionTask) legacyTaskSlot(segments []*SegmentInfo) int64 {
	isSort := t.GetTaskProto().GetType() == datapb.CompactionType_SortCompaction
	slotUsage := paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64()
	if isSort && len(segments) > 0 {
		segSize := segments[0].getSegmentSize()
		slotUsage = calculateStatsTaskSlot(segSize)
		mlog.Info(context.TODO(), "mixCompactionTask get legacy task slot",
			mlog.Int64("segmentSize", segSize), mlog.Int64("taskSlot", slotUsage))
	}
	if isSort && len(segments) == 0 {
		// A sort compaction whose segment did not resolve would otherwise be
		// pinned at the mix constant for the rest of this task instance's life,
		// because the cache short-circuits every later call.
		return slotUsage
	}
	t.slotUsage.Store(slotUsage)
	return slotUsage
}

func (t *mixCompactionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *mixCompactionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *mixCompactionTask) GetTaskVersion() int64 {
	return int64(t.GetTaskProto().GetRetryTimes())
}

func (t *mixCompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	plan, err := t.BuildCompactionRequest()
	if err != nil {
		mlog.Warn(context.TODO(), "mixCompactionTask failed to build compaction request", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			mlog.Warn(context.TODO(), "mixCompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}

	err = cluster.CreateCompaction(nodeID, plan, t.GetTaskProto().GetCollectionID())
	if err != nil {
		// Compaction tasks may be refused by DataNode because of slot limit. In this case, the node id is reset
		//  to enable a retry in compaction.checkCompaction().
		// This is tricky, we should remove the reassignment here.
		originNodeID := t.GetTaskProto().GetNodeID()
		mlog.Warn(context.TODO(), "mixCompactionTask failed to notify compaction tasks to DataNode",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.Int64("nodeID", originNodeID),
			mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			mlog.Warn(context.TODO(), "mixCompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", originNodeID), t.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
		return
	}
	mlog.Info(context.TODO(), "mixCompactionTask notify compaction tasks to DataNode")

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing), setNodeID(nodeID))
	if err != nil {
		mlog.Warn(context.TODO(), "mixCompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
	}
}

func (t *mixCompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	result, err := cluster.QueryCompaction(t.GetTaskProto().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	})
	if err != nil || result == nil {
		mlog.Warn(context.TODO(), "mixCompactionTask failed to get compaction result", mlog.Err(err))
		if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID)); err != nil {
			mlog.Warn(context.TODO(), "mixCompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		if len(result.GetSegments()) == 0 {
			mlog.Info(context.TODO(), "compaction result is empty, all data may have been deleted")
		}
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTaskProto())
		if err != nil {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
			return
		}
		if err := t.saveSegmentMeta(result); err != nil {
			mlog.Warn(context.TODO(), "mixCompactionTask failed to save segment meta", mlog.Err(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
				if err != nil {
					mlog.Warn(context.TODO(), "mixCompactionTask failed to setState failed", mlog.Err(err))
				}
			}
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
		if err != nil {
			mlog.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_failed:
		mlog.Info(context.TODO(), "mixCompactionTask fail in datanode")
		err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			mlog.Warn(context.TODO(), "fail to updateAndSaveTaskMeta")
		}
	default:
		mlog.Error(context.TODO(), "not support compaction task state", mlog.String("state", result.GetState().String()))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			mlog.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	}
}

func (t *mixCompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	if err := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID()); err != nil {
		mlog.Warn(context.TODO(), "mixCompactionTask processCompleted unable to drop compaction plan")
	}
}

func (t *mixCompactionTask) GetTaskProto() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newMixCompactionTask(t *datapb.CompactionTask,
	allocator allocator.Allocator,
	meta CompactionMeta,
	ievm IndexEngineVersionManager,
) *mixCompactionTask {
	task := &mixCompactionTask{
		allocator: allocator,
		meta:      meta,
		ievm:      ievm,
		times:     taskcommon.NewTimes(),
	}
	task.taskProto.Store(t)
	return task
}

func (t *mixCompactionTask) processMetaSaved() bool {
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		mlog.Warn(context.TODO(), "mixCompactionTask failed to proccessMetaSaved", mlog.Err(err))
		return false
	}

	return t.processCompleted()
}

func (t *mixCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(context.TODO(), task)
}

func (t *mixCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskProto())
}

func (t *mixCompactionTask) saveSegmentMeta(result *datapb.CompactionPlanResult) error {
	if err := binlog.CompressCompactionBinlogs(result.GetSegments()); err != nil {
		return err
	}
	// Also prepare metric updates.
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(context.TODO(), t.taskProto.Load().(*datapb.CompactionTask), result)
	if err != nil {
		return err
	}
	// Apply metrics after successful meta update.
	newSegmentIDs := lo.Map(newSegments, func(s *SegmentInfo, _ int) UniqueID { return s.GetID() })
	metricMutation.commit()
	for _, newSegID := range newSegmentIDs {
		select {
		case getBuildIndexChSingleton() <- newSegID:
		default:
		}
	}

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setResultSegments(newSegmentIDs))
	if err != nil {
		mlog.Warn(context.TODO(), "mixCompaction failed to setState meta saved", mlog.Err(err))
		return err
	}
	mlog.Info(context.TODO(), "mixCompactionTask success to save segment meta")
	return nil
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed or Timeout
func (t *mixCompactionTask) Process() bool {
	lastState := t.GetTaskProto().GetState().String()
	processResult := false
	switch t.GetTaskProto().GetState() {
	case datapb.CompactionTaskState_meta_saved:
		processResult = t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		processResult = t.processCompleted()
	case datapb.CompactionTaskState_failed:
		processResult = t.processFailed()
	case datapb.CompactionTaskState_timeout:
		processResult = true
	}
	currentState := t.GetTaskProto().GetState().String()
	if currentState != lastState {
		mlog.Info(context.TODO(), "mix compaction task state changed", mlog.String("lastState", lastState), mlog.String("currentState", currentState))
	}
	return processResult
}

func (t *mixCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.taskProto.Load().(*datapb.CompactionTask).PartitionID, t.GetTaskProto().GetChannel())
}

func (t *mixCompactionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (t.GetTaskProto().GetNodeID() == 0 || t.GetTaskProto().GetNodeID() == NullNodeID)
}

func (t *mixCompactionTask) processCompleted() bool {
	t.resetSegmentCompacting()
	mlog.Info(context.TODO(), "mixCompactionTask processCompleted done")
	return true
}

func (t *mixCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(context.TODO(), t.taskProto.Load().(*datapb.CompactionTask).GetInputSegments(), false)
}

func (t *mixCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t.GetTaskProto()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *mixCompactionTask) processFailed() bool {
	return true
}

func (t *mixCompactionTask) Clean() bool {
	return t.doClean() == nil
}

func (t *mixCompactionTask) doClean() error {
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		mlog.Warn(context.TODO(), "mixCompactionTask fail to updateAndSaveTaskMeta", mlog.Err(err))
		return err
	}
	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	mlog.Info(context.TODO(), "mixCompactionTask clean done")
	return nil
}

func (t *mixCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	// if task state is completed, cleaned, failed, timeout, then do append end time and save
	if t.GetTaskProto().State == datapb.CompactionTaskState_completed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_cleaned ||
		t.GetTaskProto().State == datapb.CompactionTaskState_failed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_timeout {
		ts := time.Now().Unix()
		opts = append(opts, setEndTime(ts))
	}

	task := t.ShadowClone(opts...)
	err := t.saveTaskMeta(task)
	if err != nil {
		return err
	}
	t.SetTask(task)
	return nil
}

func (t *mixCompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *mixCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *mixCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	taskSchema := taskProto.GetSchema()
	if taskSchema == nil {
		return nil, merr.WrapErrIllegalCompactionPlan("compaction task schema is nil")
	}
	taskSchemaVersion := taskSchema.GetVersion()
	compactionParams, err := compaction.GenerateJSONParams(taskSchema)
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:                    taskProto.GetPlanID(),
		StartTime:                 taskProto.GetStartTime(),
		Type:                      taskProto.GetType(),
		Channel:                   taskProto.GetChannel(),
		CollectionTtl:             taskProto.GetCollectionTtl(),
		TotalRows:                 taskProto.GetTotalRows(),
		Schema:                    taskSchema,
		PreAllocatedSegmentIDs:    taskProto.GetPreAllocatedSegmentIDs(),
		MaxSize:                   taskProto.GetMaxSize(),
		JsonParams:                compactionParams,
		CurrentScalarIndexVersion: t.ievm.ResolveScalarIndexVersion(),
	}

	// Both SortCompaction and MixCompaction build text indexes inline and need the analyzer resources in ref mode.
	if fileresource.IsRefMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()) &&
		(taskProto.GetType() == datapb.CompactionType_SortCompaction || taskProto.GetType() == datapb.CompactionType_MixCompaction) &&
		len(taskSchema.GetFileResourceIds()) > 0 {
		resources, err := t.meta.GetFileResources(context.Background(), taskSchema.GetFileResourceIds()...)
		if err != nil {
			mlog.Warn(context.TODO(), "get file resources for collection failed", mlog.Int64("collectionID", taskProto.GetCollectionID()), mlog.Err(err))
			return nil, merr.Wrap(err, "get file resources for compaction failed")
		}
		plan.FileResources = resources
	}

	segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
	segments := make([]*SegmentInfo, 0, len(taskProto.GetInputSegments()))
	for _, segID := range taskProto.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(context.TODO(), segID)
		if segInfo == nil {
			return nil, merr.WrapErrSegmentNotFound(segID)
		}
		if taskSchemaVersion < segInfo.GetSchemaVersion() {
			return nil, merr.WrapErrIllegalCompactionPlanMsg("compaction task schema version %d is older than input segment %d schema version %d", taskSchemaVersion, segInfo.GetID(), segInfo.GetSchemaVersion())
		}
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:           segID,
			CollectionID:        segInfo.GetCollectionID(),
			PartitionID:         segInfo.GetPartitionID(),
			Level:               segInfo.GetLevel(),
			InsertChannel:       segInfo.GetInsertChannel(),
			FieldBinlogs:        segInfo.GetBinlogs(),
			Field2StatslogPaths: segInfo.GetStatslogs(),
			Deltalogs:           segInfo.GetDeltalogs(),
			IsSorted:            segInfo.GetIsSorted(),
			IsSortedByNamespace: segInfo.GetIsSortedByNamespace(),
			StorageVersion:      segInfo.GetStorageVersion(),
			Manifest:            segInfo.GetManifestPath(),
			CommitTimestamp:     segInfo.GetCommitTimestamp(),
		})
		segIDMap[segID] = segInfo.GetDeltalogs()
		segments = append(segments, segInfo)
	}
	// Every input segment above either resolved or the function already
	// returned, so segments is complete: reuse it here instead of making
	// GetSlotUsage do its own independent meta fetch, which would both
	// double the lookups and open a window where the two fetches disagree
	// about which segments actually exist.
	plan.TaskResources = t.computeAndCacheRequirement(segments, true).ToProto()
	// The scalar keeps its original meaning for a worker that predates the
	// vector; see GetTaskSlot.
	plan.SlotUsage = t.legacyTaskSlot(segments)

	logIDRange, err := PreAllocateBinlogIDs(t.allocator, segments, taskSchema)
	if err != nil {
		return nil, err
	}
	plan.PreAllocatedLogIDs = logIDRange
	// BeginLogID is deprecated, but still assign it for compatibility.
	plan.BeginLogID = logIDRange.Begin

	WrapPluginContext(taskProto.GetCollectionID(), taskSchema.GetProperties(), plan)

	mlog.Info(context.TODO(), "Compaction handler refreshed mix compaction plan", mlog.Int64("maxSize", plan.GetMaxSize()),
		mlog.Any("PreAllocatedLogIDs", logIDRange), mlog.Any("segID2DeltaLogs", segIDMap))
	return plan, nil
}

func (t *mixCompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}
