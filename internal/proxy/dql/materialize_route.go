package dql

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/resource"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// materializeInScope: a search requery, whose output follows the search ids,
// so the arrival order of a reply is not visible to the client. Filter queries
// and Get stay on Query, which returns rows in pk order. Also no limit, no
// offset, no ORDER BY, no aggregation or GROUP BY, not an iterator, not
// element-level, and every output field carried as Arrow (materialize.Carries).
func (t *QueryTask) materializeInScope() bool {
	p := t.queryParams
	if !t.reQuery || p == nil || p.limit != typeutil.Unlimited || p.offset != 0 || p.isIterator ||
		len(p.orderByFields) > 0 || len(t.GetGroupByFieldIds()) > 0 || len(t.GetAggregates()) > 0 {
		return false
	}
	// Segcore makes a retrieve element-level exactly when the top-level
	// predicate is an element filter; a batch cannot carry ElementIndices.
	if t.plan.GetQuery().GetPredicates().GetElementFilterExpr() != nil {
		return false
	}
	for _, id := range t.GetOutputFieldsId() {
		if id == common.TimeStampField || id == common.RowIDField {
			continue
		}
		f, err := t.schema.SchemaHelper.GetFieldFromID(id)
		if err != nil || !materialize.Carries(f.GetDataType()) {
			return false
		}
	}
	return true
}

// newMaterializeFill returns the request's fill, with the Arrow schema the
// proxy already holds for it resolved once, for every shard: an eviction
// meanwhile cannot strand a reply that omitted the bytes.
func (t *QueryTask) newMaterializeFill() *materializeFill {
	key := newMaterializeSchemaKey(t.GetCollectionID(), t.schema.GetVersion(), t.GetOutputFieldsId())
	return newMaterializeFill(t.schema.SchemaHelper, paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64(),
		t.ids, materializeSchemas.resolve(key), &key)
}

// The path label of ProxyQueryExecuteLatency.
const (
	executePathQuery       = "query"
	executePathMaterialize = "materialize"
	// executePathFallback: Materialize, then the whole request again on Query.
	executePathFallback = "fallback"
)

// executeMaterialize runs the request with Materialize and, if any shard
// could not serve it, reruns the whole request with Query, so Materialize and
// Query results are never mixed (design doc 20261019 section 8). A fallback
// wins over another shard's error: the rerun meets that error again if it
// persists.
// It returns the path that served the request.
func (t *QueryTask) executeMaterialize(ctx context.Context) (string, error) {
	fill := t.newMaterializeFill()
	err := t.executeWorkload(ctx, func(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
		return t.materializeShard(ctx, nodeID, qn, channel, fill)
	})
	if reason, failure := fill.failure(); reason != "" {
		// Nothing read from a Materialize reply survives.
		mlog.Info(ctx, "Materialize unsupported, rerunning the request with Query",
			mlog.Int64("collection", t.GetCollectionID()),
			mlog.String("reason", reason),
			mlog.Err(failure),
			mlog.NamedError("shardErr", err))
		recordMaterializeFallback(reason)
		fill.release()
		return executePathFallback, t.executeWorkload(ctx, t.queryShard)
	}
	if err != nil {
		fill.release()
		return executePathMaterialize, err
	}
	return executePathMaterialize, t.finishMaterialize(ctx, fill)
}

// materializeShard is queryShard for Materialize; the reply is decoded here,
// on arrival, into fill. An unsupported node (Unimplemented) and a
// proxy-side failure (a body or schema it cannot read, a duplicate pk across
// shards) do not fail the shard: the error is kept on fill and
// nil returned, so the load balancer neither blacklists the delegator nor
// retries another replica. executeMaterialize then reruns the request with
// Query or fails it (design doc 20261019 sections 8 and 9). A reply read is
// only recorded: its side effects (shardDone) apply once the request ends on
// Materialize.
func (t *QueryTask) materializeShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string, fill *materializeFill) error {
	ctx = retry.WithMaxAttemptsContext(ctx, 1)
	req, skip := t.shardRequest(nodeID, channel)
	if skip {
		return nil
	}
	req.MaterializeSchemaFingerprint = fill.known.fingerprint
	req.MaterializeFormatVersion = materialize.FormatVersion
	log := mlog.With(mlog.Int64("collection", t.GetCollectionID()),
		mlog.Int64s("partitionIDs", t.GetPartitionIDs()),
		mlog.Int64("nodeID", nodeID),
		mlog.String("channel", channel))

	result, err := qn.Materialize(ctx, req)
	status := result.GetHeader().GetStatus()
	serr := err
	if serr == nil {
		serr = merr.Error(status)
	}
	if serr != nil {
		// A failed reply carries no body; free whatever the codec attached.
		resource.ReleaseMaterializeBody(result)
		// A node without the RPC fails the call; a node that cannot serve
		// this request answers with the status.
		if errors.Is(serr, merr.ErrServiceUnimplemented) {
			reason := fallbackUnsupported
			if err != nil {
				reason = fallbackUnimplemented
			}
			log.Info(ctx, "Materialize unsupported, the request will rerun with Query", mlog.String("reason", reason), mlog.Err(serr))
			fill.fallBack(reason, serr)
			return nil
		}
		return t.shardReplyError(ctx, log, "Materialize", nodeID, status, err)
	}

	// add takes the reply's body; the fill frees it after finish.
	if err := fill.add(nodeID, channel, result); err != nil {
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			log.Info(ctx, "Materialize replies disagree on the schema, the request will rerun with Query", mlog.Err(err))
			fill.fallBack(fallbackSchemaSkew, err)
		} else {
			log.Warn(ctx, "failed to read materialize reply", mlog.Err(err))
			fill.fail(err)
		}
	}
	return nil
}

// finishMaterialize ends a Materialize fan-out that needs no fallback: it
// fails the request with the first proxy-side error, or puts the fill's
// result in resultBuf, where PostExecute finds it as the only result, and
// applies the shards' side effects. It releases the fill.
func (t *QueryTask) finishMaterialize(ctx context.Context, fill *materializeFill) error {
	defer fill.release()
	if _, err := fill.failure(); err != nil {
		mlog.Warn(ctx, "failed to read materialize result",
			mlog.Int64("collection", t.GetCollectionID()),
			mlog.Err(err))
		return err
	}
	res, err := fill.finish()
	if err != nil {
		mlog.Warn(ctx, "failed to fill materialize result",
			mlog.Int64("collection", t.GetCollectionID()),
			mlog.Err(err))
		return err
	}
	t.resultBuf.Insert(res)
	for _, a := range fill.accepted {
		t.shardDone(a.nodeID, a.channel, a.mvcc, a.cost)
	}
	metrics.ProxyMaterializeShardTotal.WithLabelValues(paramtable.GetStringNodeID()).Add(float64(len(fill.accepted)))
	return nil
}

func recordMaterializeFallback(reason string) {
	metrics.ProxyMaterializeFallbackTotal.WithLabelValues(paramtable.GetStringNodeID(), reason).Inc()
}
