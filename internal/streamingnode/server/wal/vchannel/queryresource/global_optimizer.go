package queryresource

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/views/optimizer"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

type idfOracle interface {
	BuildIDFBatch([]IDFRequest) ([]IDFResult, error)
}

type globalOptimizer struct {
	idf               idfOracle
	functionRunnerKey string
}

func NewGlobalOptimizer(runtime *QueryRuntime, functionRunnerKey string) optimizer.GlobalOptimizer {
	return globalOptimizer{
		idf:               findIDFOracle(runtime),
		functionRunnerKey: functionRunnerKey,
	}
}

func findIDFOracle(runtime *QueryRuntime) idfOracle {
	if runtime == nil {
		return nil
	}
	var oracle idfOracle
	runtime.RangeModules(func(module QueryRuntimeModule) bool {
		if idf, ok := module.(idfOracle); ok {
			oracle = idf
			return false
		}
		return true
	})
	return oracle
}

func (o globalOptimizer) OptimizeSearch(ctx context.Context, req *internalpb.SearchRequest) (optimizer.SearchOptimization, error) {
	if req == nil {
		return optimizer.SearchOptimization{}, merr.WrapErrParameterInvalid("search request", "nil")
	}
	if req.GetIsAdvanced() {
		return o.optimizeAdvancedSearch(ctx, req)
	}
	return o.optimizeSearch(ctx, req)
}

// IDFRequest/IDFResult batch all BM25 subqueries against one aggregate state.
type IDFRequest struct {
	FieldID int64
	TFs     *schemapb.SparseFloatArray
}
type IDFResult struct {
	Vectors [][]byte
	Avgdl   float64
}

func (o globalOptimizer) optimizeAdvancedSearch(ctx context.Context, req *internalpb.SearchRequest) (optimizer.SearchOptimization, error) {
	if len(req.GetSubReqs()) == 0 {
		return optimizer.SearchOptimization{}, merr.WrapErrServiceInternalMsg("advanced search request has no sub-requests")
	}
	parent := proto.Clone(req).(*internalpb.SearchRequest)
	parent.SubReqs = nil
	requests := make([]*internalpb.SearchRequest, len(req.GetSubReqs()))
	for i, sub := range req.GetSubReqs() {
		request, err := sharedviewquery.BuildSubSearchRequest(parent, sub)
		if err != nil {
			return optimizer.SearchOptimization{}, err
		}
		requests[i] = request
	}
	if err := o.optimizeRequests(ctx, requests); err != nil {
		return optimizer.SearchOptimization{}, err
	}
	for i, sub := range req.GetSubReqs() {
		if err := sharedviewquery.UpdateSubSearchRequest(sub, requests[i], false); err != nil {
			return optimizer.SearchOptimization{}, err
		}
	}
	return optimizer.SearchOptimization{}, nil
}

func (o globalOptimizer) optimizeSearch(ctx context.Context, req *internalpb.SearchRequest) (optimizer.SearchOptimization, error) {
	return optimizer.SearchOptimization{}, o.optimizeRequests(ctx, []*internalpb.SearchRequest{req})
}

func (globalOptimizer) OptimizeRetrieve(context.Context, *internalpb.RetrieveRequest) error {
	return nil
}

func (o globalOptimizer) optimizeRequests(ctx context.Context, requests []*internalpb.SearchRequest) error {
	inputs := make([]IDFRequest, 0, len(requests))
	bm25Requests := make([]*internalpb.SearchRequest, 0, len(requests))
	for _, req := range requests {
		optimized := false
		_, err := function.GetManager().RunWithRunner(ctx, req.GetCollectionID(), o.functionRunnerKey, req.GetFieldId(), func(runner function.FunctionRunner) error {
			if runner.GetSchema().GetType() != schemapb.FunctionType_BM25 {
				return nil
			}
			if req.GetMetricType() != metric.BM25 && req.GetMetricType() != metric.EMPTY {
				return merr.WrapErrParameterInvalid("BM25", req.GetMetricType(), "must use BM25 metric type when searching against BM25 Function output field")
			}
			holder, err := parseBM25Placeholder(req)
			if err != nil {
				return err
			}
			tfs, err := buildBM25TermFrequency(ctx, req, holder, runner)
			if err != nil {
				return err
			}
			inputs = append(inputs, IDFRequest{FieldID: req.GetFieldId(), TFs: tfs})
			bm25Requests = append(bm25Requests, req)
			optimized = true
			return nil
		})
		if err != nil {
			return err
		}
		if !optimized && req.GetMetricType() == metric.BM25 {
			return merr.WrapErrServiceInternalMsg("BM25 function runner is not initialized for field: %d", req.GetFieldId())
		}
	}
	if len(inputs) == 0 {
		return nil
	}
	if o.idf == nil {
		return merr.WrapErrServiceInternalMsg("BM25 IDF oracle is not initialized")
	}
	results, err := o.idf.BuildIDFBatch(inputs)
	if err != nil {
		return merr.Wrap(err, "build BM25 IDF")
	}
	for i, req := range bm25Requests {
		if err := setBM25Params(req, results[i].Avgdl); err != nil {
			return err
		}
		req.PlaceholderGroup = funcutil.SparseVectorDataToPlaceholderGroupBytes(results[i].Vectors)
	}
	return nil
}

func parseBM25Placeholder(req *internalpb.SearchRequest) (*commonpb.PlaceholderValue, error) {
	pb := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(req.GetPlaceholderGroup(), pb); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("failed to unmarshal BM25 IDF placeholder group: %v", err)
	}
	if len(pb.Placeholders) != 1 || len(pb.Placeholders[0].Values) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("please provide varchar/text for BM25 Function based search")
	}
	holder := pb.Placeholders[0]
	if holder.Type != commonpb.PlaceholderType_VarChar {
		return nil, merr.WrapErrParameterInvalidMsg("please provide varchar/text for BM25 Function based search, got %s", holder.Type.String())
	}
	return holder, nil
}

func buildBM25TermFrequency(ctx context.Context, req *internalpb.SearchRequest, holder *commonpb.PlaceholderValue, functionRunner function.FunctionRunner) (*schemapb.SparseFloatArray, error) {
	texts := funcutil.GetVarCharFromPlaceholder(holder)
	datas := []any{texts}
	if len(functionRunner.GetInputFields()) == 2 {
		analyzerName := "default"
		if name := req.GetAnalyzerName(); name != "" {
			analyzerName = name
		}
		analyzers := make([]string, len(texts))
		for i := range texts {
			analyzers[i] = analyzerName
		}
		datas = append(datas, analyzers)
	}
	output, err := functionRunner.BatchRun(datas...)
	if err != nil {
		return nil, merr.WrapErrFunctionFailed(err, "BM25 embedding failed")
	}
	if len(output) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("BM25 embedding failed: runner returned empty output")
	}
	tfArray, ok := output[0].(*schemapb.SparseFloatArray)
	if !ok {
		return nil, merr.WrapErrFunctionFailedMsg("functionRunner return unknown data")
	}
	return tfArray, nil
}

func setBM25Params(req *internalpb.SearchRequest, avgdl float64) error {
	serializedPlan := req.GetSerializedExprPlan()
	if serializedPlan == nil {
		return merr.WrapErrParameterInvalid("serialized search plan", "nil")
	}
	plan := planpb.PlanNode{}
	if err := proto.Unmarshal(serializedPlan, &plan); err != nil {
		return merr.WrapErrParameterInvalid("valid serialized search plan", "no unmarshalable one", err.Error())
	}
	switch plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		plan.GetVectorAnns().GetQueryInfo().Bm25Avgdl = avgdl
		serializedExprPlan, err := proto.Marshal(&plan)
		if err != nil {
			return merr.WrapErrParameterInvalid("marshalable search plan", "plan with marshal error", err.Error())
		}
		req.SerializedExprPlan = serializedExprPlan
	}
	return nil
}
