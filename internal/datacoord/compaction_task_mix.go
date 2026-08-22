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
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ CompactionTask = (*mixCompactionTask)(nil)

type mixCompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask

	// ctx is the inspector's process context, threaded from buildCompactTask
	// so the scheduler callbacks (which receive none) can still log with it.
	ctx context.Context

	allocator allocator.Allocator
	meta      CompactionMeta

	ievm IndexEngineVersionManager

	times *taskcommon.Times

	slotUsage atomic.Int64
}

func (t *mixCompactionTask) GetTaskID() int64 {
	return t.GetTask().GetPlanID()
}

func (t *mixCompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *mixCompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTask().GetState())
}

func (t *mixCompactionTask) GetTaskSlot() int64 {
	slotUsage := t.slotUsage.Load()
	if slotUsage == 0 {
		slotUsage = paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64()
		if t.GetTask().GetType() == datapb.CompactionType_SortCompaction {
			segment := t.meta.GetHealthySegment(context.Background(), t.GetTask().GetInputSegments()[0])
			if segment != nil {
				segSize := segment.getSegmentSize()
				slotUsage = calculateStatsTaskSlot(segSize)
				mlog.Info(t.ctx, "mixCompactionTask get task slot",
					mlog.Int64("segment size", segSize), mlog.Int64("task slot", slotUsage))
			}
		}
		t.slotUsage.Store(slotUsage)
	}
	return slotUsage
}

func (t *mixCompactionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *mixCompactionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *mixCompactionTask) GetTaskVersion() int64 {
	return int64(t.GetTask().GetRetryTimes())
}

func (t *mixCompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	if callbackPreempted(t) {
		return
	}

	plan, err := t.BuildCompactionRequest()
	if err != nil {
		mlog.Warn(t.ctx, "mixCompactionTask failed to build compaction request", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error()))
		if err != nil {
			mlog.Warn(t.ctx, "mixCompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}

	// Persist the assignment, send the plan, and classify the outcome. See
	// dispatchCompactionPlan for the ordering and the outcome rules.
	if err := dispatchCompactionPlan(t.ctx, t, nodeID, cluster, plan, "mixCompactionTask"); err != nil {
		mlog.Warn(t.ctx, "mixCompactionTask failed to persist assignment, not sending plan",
			mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.FieldNodeID(nodeID), mlog.Err(err))
	}
}

func (t *mixCompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	if callbackPreempted(t) {
		return
	}

	if hasNoWorker(t) {
		return
	}

	result, err := cluster.QueryCompaction(t.GetTask().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTask().GetPlanID(),
	})
	if err != nil || result == nil {
		log := mlog.With(mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.FieldNodeID(t.GetTask().GetNodeID()))
		// A query never carries a refusal: either the worker answered about this
		// plan, or we learned nothing. Learning nothing does not mean the worker
		// stopped, so the plan is never handed to a second node -- we either wait
		// for the next round or abandon the attempt for a replan.
		//
		// A nil result with no error is treated the same way. The current
		// DataNode always answers a non-zero plan ID with exactly one result
		// carrying that ID, synthesizing a failed one when it does not know the
		// plan, so nil should be unreachable -- but "unreachable" is a property
		// of the peer's code, not of ours.
		if errors.Is(err, merr.ErrNodeNotFound) {
			// DataCoord's registry lost the node. That says nothing about the
			// process, which deregisters from etcd before tearing down its
			// running compactions -- so a rolling upgrade lands here while the
			// worker is still writing. Give up on this attempt, replan.
			log.Warn(t.ctx, "mixCompactionTask worker left the cluster, abandoning attempt for replan", mlog.Err(err))
			abandonAttempt(t.ctx, t, "assigned worker left the cluster")
			return
		}
		// One rule for every RPC round, same as the create path: a round that
		// ends without an answer ends the attempt. The query already spent
		// dataCoord.requestTimeoutSeconds on an operation a healthy worker
		// answers in microseconds; waiting more rounds holds the inputs locked
		// with no convergence in sight, since a node that keeps renewing its
		// session while black-holing RPCs never turns into ErrNodeNotFound.
		// Abandoning is cheap by design -- the replan cannot collide with
		// whatever the worker is still doing -- and dataCoord.compaction.maxAttempts
		// bounds the churn.
		log.Warn(t.ctx, "mixCompactionTask query unanswered, abandoning attempt for replan", mlog.Err(err))
		abandonAttempt(t.ctx, t, "worker left the query unanswered")
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		if len(result.GetSegments()) == 0 {
			mlog.Info(t.ctx, "compaction result is empty, all data may have been deleted")
		}
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTask())
		if err != nil {
			// The worker says it finished, we say its inputs are no longer in a
			// state we can commit against. That ends the attempt, so it has to
			// be visible: the only other record of it is a fail_reason nobody
			// reads until they already suspect this task.
			mlog.Warn(t.ctx, "mixCompactionTask rejected a completed result, ending the attempt",
				mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
			if saveErr := t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error())); saveErr != nil {
				mlog.Warn(t.ctx, "mixCompactionTask failed to persist the rejected result",
					mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(saveErr))
			}
			return
		}
		if err := t.saveSegmentMeta(result); err != nil {
			mlog.Warn(t.ctx, "mixCompactionTask failed to save segment meta", mlog.Err(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error()))
				if err != nil {
					mlog.Warn(t.ctx, "mixCompactionTask failed to setState failed", mlog.Err(err))
				}
			}
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason("worker reported timeout"))
		if err != nil {
			mlog.Warn(t.ctx, "update mix compaction task meta failed", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_failed:
		// Keep the worker's reason. Recording only the state leaves fail_reason
		// empty, so a plan that keeps failing gives a reviewer nothing to work
		// from beyond "it failed".
		reason := workerCompactionFailReason(result.GetReason())
		mlog.Warn(t.ctx, "mixCompactionTask fail in datanode",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("reason", reason))
		err := t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(reason))
		if err != nil {
			mlog.Warn(t.ctx, "fail to updateAndSaveTaskMeta", mlog.Err(err))
		}
	default:
		mlog.Error(t.ctx, "not support compaction task state", mlog.String("state", result.GetState().String()))
		err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason("unsupported worker state "+result.GetState().String()))
		if err != nil {
			mlog.Warn(t.ctx, "update mix compaction task meta failed", mlog.Err(err))
			return
		}
	}
}

func (t *mixCompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	if err := cluster.DropCompaction(t.GetTask().GetNodeID(), t.GetTask().GetPlanID()); err != nil {
		mlog.Warn(t.ctx, "mixCompactionTask processCompleted unable to drop compaction plan", mlog.Err(err))
	}
}

func (t *mixCompactionTask) GetTask() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newMixCompactionTask(ctx context.Context, t *datapb.CompactionTask,
	allocator allocator.Allocator,
	meta CompactionMeta,
	ievm IndexEngineVersionManager,
) *mixCompactionTask {
	task := &mixCompactionTask{
		ctx:       ctx,
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
		mlog.Warn(t.ctx, "mixCompactionTask failed to proccessMetaSaved", mlog.Err(err))
		return false
	}

	return t.processCompleted()
}

func (t *mixCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(t.ctx, task)
}

func (t *mixCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTask())
}

func (t *mixCompactionTask) saveSegmentMeta(result *datapb.CompactionPlanResult) error {
	if err := binlog.CompressCompactionBinlogs(result.GetSegments()); err != nil {
		return err
	}
	// Also prepare metric updates.
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(t.ctx, t.taskProto.Load().(*datapb.CompactionTask), result)
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
		mlog.Warn(t.ctx, "mixCompaction failed to setState meta saved", mlog.Err(err))
		return err
	}
	mlog.Info(t.ctx, "mixCompactionTask success to save segment meta")
	return nil
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed or Timeout
func (t *mixCompactionTask) Process() bool {
	lastState := t.GetTask().GetState().String()
	processResult := false
	switch t.GetTask().GetState() {
	case datapb.CompactionTaskState_meta_saved:
		processResult = t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		processResult = t.processCompleted()
	case datapb.CompactionTaskState_failed:
		processResult = t.processFailed()
	case datapb.CompactionTaskState_timeout, datapb.CompactionTaskState_retrying:
		processResult = true
	}
	currentState := t.GetTask().GetState().String()
	if currentState != lastState {
		mlog.Info(t.ctx, "mix compaction task state changed", mlog.String("lastState", lastState), mlog.String("currentState", currentState))
	}
	return processResult
}

func (t *mixCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.taskProto.Load().(*datapb.CompactionTask).PartitionID, t.GetTask().GetChannel())
}

func (t *mixCompactionTask) NeedReAssignNodeID() bool {
	return t.GetTask().GetState() == datapb.CompactionTaskState_pipelining && hasNoWorker(t)
}

func (t *mixCompactionTask) processCompleted() bool {
	// Releasing the input segments is deliberately left to doClean, which is the
	// single place that does it. A completed task always reaches cleanup, and
	// unlocking here as well would mean unlocking twice: by the second call the
	// segments may already belong to another compaction that legitimately
	// re-acquired them.
	mlog.Info(t.ctx, "mixCompactionTask processCompleted done")
	return true
}

func (t *mixCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.ctx, t.taskProto.Load().(*datapb.CompactionTask).GetInputSegments(), false)
}

func (t *mixCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t.GetTask()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *mixCompactionTask) processFailed() bool {
	return true
}

func (t *mixCompactionTask) Clean() bool {
	// Runs under the scheduler's per-task lock, handed over by Finalize.
	if alreadyCleaned(t) {
		return true
	}
	return t.doClean() == nil
}

// doClean settles what a terminated task owes its inputs. For a sort task that
// is exactly one thing: give the segment lock back, so the segment becomes
// eligible for another sort. It deliberately does not touch IsInvisible --
// that flag means "this segment still owes a sort", and its only exit is a
// sorted replacement being published. Cleanup releasing it would put an
// unsorted, unindexable segment on the sealed query path (index_inspector.go
// skips unsorted segments) instead of letting the trigger re-sort it.
func (t *mixCompactionTask) getCtx() context.Context {
	return t.ctx
}

func (t *mixCompactionTask) doClean() error {
	// The cleaned write and the input release live in finishClean: the release
	// must be the last step of Clean, or a retried clean could unlock inputs a
	// second compaction has legitimately re-acquired.
	return finishClean(t, "mixCompactionTask")
}

func (t *mixCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	return updateAndSaveCompactionTaskMeta(t, opts...)
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
		SlotUsage:                 t.GetSlotUsage(),
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
			mlog.Warn(t.ctx, "get file resources for collection failed", mlog.Int64("collectionID", taskProto.GetCollectionID()), mlog.Err(err))
			return nil, merr.Wrap(err, "get file resources for compaction failed")
		}
		plan.FileResources = resources
	}

	segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
	segments := make([]*SegmentInfo, 0, len(taskProto.GetInputSegments()))
	for _, segID := range taskProto.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(t.ctx, segID)
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

	logIDRange, err := PreAllocateBinlogIDs(t.allocator, segments, taskSchema)
	if err != nil {
		return nil, err
	}
	plan.PreAllocatedLogIDs = logIDRange
	// BeginLogID is deprecated, but still assign it for compatibility.
	plan.BeginLogID = logIDRange.Begin

	WrapPluginContext(taskProto.GetCollectionID(), taskSchema.GetProperties(), plan)

	mlog.Info(t.ctx, "Compaction handler refreshed mix compaction plan", mlog.Int64("maxSize", plan.GetMaxSize()),
		mlog.Any("PreAllocatedLogIDs", logIDRange), mlog.Any("segID2DeltaLogs", segIDMap))
	return plan, nil
}

func (t *mixCompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}
