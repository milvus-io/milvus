// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// This file owns the immutable configuration of one reshard run. Every value is
// derived once from the request, the plan and the live parameters
// (deriveReshardPolicies), then read-only for the rest of the run: the fragment
// policy shapes a cut, the memory policy sizes a spill, and the write/source
// specs carry what one detached write and one source read need.

import (
	"path"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// fragmentPolicy is the immutable fragment-shaping configuration of one run:
// the decoded target a bucket crosses to trigger a cut, the Sort input budget a
// packed fragment input must stay within, and the per-fragment structural
// overhead charged on top of decoded bytes.
type fragmentPolicy struct {
	target    int64
	sortInput int64
	overhead  int64
}

// memoryPolicy is the immutable memory-accounting configuration of one run: the
// whole-task bucket-resident ceiling, the slot memory budget the shared model
// was derived from, and the model itself.
type memoryPolicy struct {
	residentBudget int64
	budget         int64
	model          reshardmem.Model
}

// writeSpec is the immutable configuration one detached fragment write needs:
// the run identity and output location, the fragment schema and sort fields, the
// packed writer's buffer size, and the run's spill area to replay spilled pieces
// from.
type writeSpec struct {
	req            *datapb.ReshardTaskRequest
	plan           *importv3pb.ReshardTaskPlan
	fragmentSchema *schemapb.CollectionSchema
	sortFields     []int64
	bufferSize     int64
	pluginContext  *indexcgopb.StoragePluginContext
	spillRoot      string
	spillStreams   int
}

// sourceSpec is the immutable configuration one source read and its prepare
// stage need: the chunk manager and per-file reader inputs, the normalize and
// function contract, and the RLS predicate.
type sourceSpec struct {
	cm               storage.ChunkManager
	collectionSchema *schemapb.CollectionSchema
	options          importutilv2.Options
	storageConfig    *indexpb.StorageConfig
	pluginContext    *indexcgopb.StoragePluginContext
	bufferSize       int64
	// backup sources already carry every function output column; ordinary
	// sources get theirs computed during the run so fragments are uniform either
	// way.
	backup       bool
	runFunctions bool
	rlsPredicate *planpb.Expr
}

// reshardPolicy bundles the immutable per-run configuration derived once from
// the request, the plan and the live parameters: what shapes a fragment, how the
// run accounts memory, how a source is read and prepared, and how a fragment is
// written.
type reshardPolicy struct {
	slot     int64
	fragment fragmentPolicy
	memory   memoryPolicy
	source   sourceSpec
	write    writeSpec
}

// getReshardPolicy reads every per-run configuration value once. It
// performs no IO: spill setup and the write pool are created by the executor.
func getReshardPolicy(req *datapb.ReshardTaskRequest, plan *importv3pb.ReshardTaskPlan) (*reshardPolicy, error) {
	backup := importutilv2.IsBackup(plan.GetOptions())
	fragmentSchema := importutilv2.FragmentSchema(plan.GetCollectionSchema(), backup)
	sortFields, err := importutilv2.SortFieldIDs(plan.GetCollectionSchema())
	if err != nil {
		return nil, err
	}
	bufferSize := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	fragmentTarget := plan.GetFragmentSize()
	// Contract: DataCoord validates a positive fragment size at plan build time.
	// A non-positive target would flush every non-empty bucket after every batch
	// (fragment count = batches x buckets), so fail loudly at the boundary too.
	if fragmentTarget <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("invalid ReshardTask fragment size %d", fragmentTarget)
	}
	slot := req.GetSlot()
	if slot <= 0 {
		slot = 1
	}
	// The task's slot budget is charged in the node's own slot unit, the same one
	// DataCoord estimated the slot with (reshardmem.MemoryPerSlot), not the
	// import-specific memoryLimitPerSlot.
	memoryBudget := slot * reshardmem.MemoryPerSlot(paramtable.Get().DataNodeCfg.WorkerSlotUnit.GetAsInt64())
	// Fixed bucket-to-shard mapping: every range of one bucket lands in the same
	// spill file, so reading a bucket back never touches the others and the
	// file/fd count stays at min(buckets, reshardSpillMaxStreams).
	bucketCount := int64(len(plan.GetVchannels()) * len(plan.GetPartitionIds()))
	memModel := reshardmem.Model{
		ReadBuffer:       bufferSize,
		FragmentTarget:   fragmentTarget,
		FlushConcurrency: paramtable.Get().DataCoordCfg.ReshardFlushConcurrency.GetAsInt64(),
		ExpansionFactor:  paramtable.Get().DataCoordCfg.ReshardMemoryExpansionFactor.GetAsFloat(),
	}
	// The task's bucket-resident ceiling: the same total the shared model charges
	// DataCoord (min(buckets, bucketCap) x fragmentTarget), applied to the bytes
	// still held by the buckets. One extra bucket per overflow is spilled (the
	// largest first) instead of forcing every bucket under an equal per-bucket
	// share, so a skewed partition hash spends the spill budget on the hot
	// buckets and keeps cold buckets resident instead of spilling them regardless
	// of the free budget. Detached fragment writes are bounded by the flush pool
	// concurrency and released by the writes, so they are outside this ceiling and
	// never trigger a spill.
	bucketCap := paramtable.Get().DataCoordCfg.ReshardResidentBucketCap.GetAsInt64()
	residentBudget := min(bucketCount, bucketCap) * fragmentTarget
	// Every routed fragment carries structural live-heap overhead on top of its
	// decoded bytes (FieldData wrappers, map entries, slice capacity and
	// allocator rounding -- see reshardmem.FragmentFieldOverhead). Accounting
	// only GetMemorySize systematically undercharges the real resident set, worst
	// when a high bucket count shreds every source batch into tiny fragments; the
	// same term is charged by DataCoord's WorkingSet.
	nFields := int64(len(typeutil.GetAllFieldSchemas(fragmentSchema)))
	fragmentOverhead := reshardmem.FragmentOverhead(nFields)
	return &reshardPolicy{
		slot: slot,
		fragment: fragmentPolicy{
			target:    fragmentTarget,
			sortInput: memModel.SortInput(memoryBudget),
			overhead:  fragmentOverhead,
		},
		memory: memoryPolicy{
			residentBudget: residentBudget,
			budget:         memoryBudget,
			model:          memModel,
		},
		source: sourceSpec{
			collectionSchema: plan.GetCollectionSchema(),
			options:          plan.GetOptions(),
			storageConfig:    req.GetStorageConfig(),
			bufferSize:       bufferSize,
			backup:           backup,
			runFunctions:     !backup,
			rlsPredicate:     plan.GetRlsCheckPredicate(),
		},
		write: writeSpec{
			req:            req,
			plan:           plan,
			fragmentSchema: fragmentSchema,
			sortFields:     sortFields,
			bufferSize:     bufferSize,
			spillRoot: path.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), importV3SpillRootDir,
				strconv.FormatInt(req.GetJobId(), 10), strconv.FormatInt(req.GetTaskId(), 10), strconv.FormatInt(req.GetRunId(), 10)),
			spillStreams: int(min(bucketCount, int64(paramtable.Get().DataNodeCfg.ReshardSpillMaxStreams.GetAsInt()))),
		},
	}, nil
}
