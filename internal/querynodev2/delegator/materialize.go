package delegator

import (
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/resource"
)

// AggregateMaterializeResults relays the workers' Materialize replies as one:
// it checks every non-empty reply has the same schema fingerprint, sums the header
// fields and forwards the body entries in the order given, without decoding
// them (design doc 20261019 §5). Costs aggregate as Query's do
// (segmentutil.AggregateWorkerCosts).
//
// It bounds the reply by maxOutputSize (<= 0: no bound) as Query's reduce
// bounds the shard's result, on the sum of the workers' result_size, each
// the reduce's measure of the rows that worker sent. The one difference: a
// pk two workers both returned (a segment handoff's transient overlap) is
// counted twice, where Query's reduce keeps one; the proxy's per-shard check
// (materializeFill.checkOutputSize) is on the deduplicated rows.
//
// No body byte is copied: the replies' bodies (the receive buffers of a
// remote worker's reply, referenced by the codec, or a local worker's Go
// buffer) are taken from the replies and attached, entry lists concatenated,
// to the output, whose marshal sends them as views (§6). On error every body
// is released.
func AggregateMaterializeResults(results []*internalpb.MaterializeResults, localNodeID int64, allWorkerCosts bool, maxOutputSize int64) (*internalpb.MaterializeResults, error) {
	bodies := make([]*resource.MaterializeBody, len(results))
	for i, r := range results {
		bodies[i] = resource.TakeMaterializeBody(r)
	}
	body := &resource.MaterializeBody{}
	fail := func(err error) (*internalpb.MaterializeResults, error) {
		for _, b := range bodies {
			b.Release()
		}
		body.Release()
		return nil, err
	}

	out := &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{Status: merr.Success()}}
	h := out.Header
	for i, r := range results {
		rh := r.GetHeader()
		if bodies[i].Len() > 0 {
			if err := relaySchema(h, rh, body.Len() == 0); err != nil {
				return fail(err)
			}
		}
		h.ResultSize += rh.GetResultSize()
		if maxOutputSize > 0 && h.ResultSize > maxOutputSize {
			return fail(merr.WrapErrParameterInvalidMsg("query results exceed the maxOutputSize Limit %d", maxOutputSize))
		}
		body.Append(bodies[i])
		h.BodyRawSize += rh.GetBodyRawSize()
		h.AllRetrieveCount += rh.GetAllRetrieveCount()
		h.ScannedRemoteBytes += rh.GetScannedRemoteBytes()
		h.ScannedTotalBytes += rh.GetScannedTotalBytes()
		h.FeatureBits |= rh.GetFeatureBits()
	}
	h.CostAggregation = segmentutil.AggregateWorkerCosts(results, func(r *internalpb.MaterializeResults) (int64, *internalpb.CostAggregation) {
		return r.GetHeader().GetSourceId(), r.GetHeader().GetCostAggregation()
	}, localNodeID, allWorkerCosts)
	resource.SetMaterializeBody(out, body)
	return out, nil
}

// relaySchema folds one non-empty reply's schema into the output header h.
// Every non-empty reply must name the same schema fingerprint. Workers send
// the schema bytes only when the request did not name that fingerprint, so
// the output carries the first bytes any worker sent, or none; the proxy
// checks them against the fingerprint.
func relaySchema(h, rh *internalpb.MaterializeHeader, first bool) error {
	fp := rh.GetSchemaFingerprint()
	if first {
		h.SchemaFingerprint = fp
	} else if h.SchemaFingerprint != fp {
		return merr.Wrap(merr.ErrServiceUnimplemented, "materialize: workers returned different Arrow schemas")
	}
	if s := rh.GetBodySchema(); len(s) > 0 && len(h.BodySchema) == 0 {
		h.BodySchema = s
	}
	return nil
}
