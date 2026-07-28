package queryresource

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/views/optimizer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

type idfOracle interface {
	BuildIDF(dataVersion qviews.DataVersion, fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error)
}

type globalOptimizer struct {
	idf               idfOracle
	dataVersion       qviews.DataVersion
	functionRunnerKey string
}

func NewGlobalOptimizer(runtime *QueryRuntime, dataVersion qviews.DataVersion, functionRunnerKey string) optimizer.GlobalOptimizer {
	return globalOptimizer{
		idf:               findIDFOracle(runtime),
		dataVersion:       dataVersion,
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
	optimized, skip, err := o.optimizeBM25(ctx, req)
	if err != nil {
		return optimizer.SearchOptimization{}, err
	}
	if !optimized && req.GetMetricType() == metric.BM25 {
		return optimizer.SearchOptimization{}, merr.WrapErrServiceInternalMsg("BM25 function runner is not initialized for field: %d", req.GetFieldId())
	}
	return optimizer.SearchOptimization{Skip: skip}, nil
}

func (globalOptimizer) OptimizeRetrieve(context.Context, *internalpb.RetrieveRequest) error {
	return nil
}

func (o globalOptimizer) optimizeBM25(ctx context.Context, req *internalpb.SearchRequest) (bool, bool, error) {
	optimized := false
	skip := false
	_, err := function.GetManager().RunWithRunner(ctx, req.GetCollectionID(), o.functionRunnerKey, req.GetFieldId(), func(functionRunner function.FunctionRunner) error {
		functionType := functionRunner.GetSchema().GetType()
		if functionType != schemapb.FunctionType_BM25 {
			return nil
		}
		if req.GetMetricType() != metric.BM25 && req.GetMetricType() != metric.EMPTY {
			return merr.WrapErrParameterInvalid("BM25", req.GetMetricType(), "must use BM25 metric type when searching against BM25 Function output field")
		}
		if o.idf == nil {
			return merr.WrapErrServiceInternalMsg("BM25 IDF oracle is not initialized")
		}
		var err error
		skip, err = o.buildBM25IDF(ctx, req, functionRunner)
		if err != nil {
			return err
		}
		optimized = true
		return nil
	})
	if err != nil {
		return false, false, err
	}
	return optimized, skip, nil
}

func (o globalOptimizer) buildBM25IDF(ctx context.Context, req *internalpb.SearchRequest, functionRunner function.FunctionRunner) (bool, error) {
	holder, err := parseBM25Placeholder(req)
	if err != nil {
		return false, err
	}
	tfArray, err := buildBM25TermFrequency(ctx, req, holder, functionRunner)
	if err != nil {
		return false, err
	}
	idfSparseVector, avgdl, err := o.idf.BuildIDF(o.dataVersion, req.GetFieldId(), tfArray)
	if err != nil {
		return false, merr.Wrap(err, "build BM25 IDF")
	}
	if avgdl <= 0 {
		return true, nil
	}
	if err := setBM25Params(req, avgdl); err != nil {
		return false, err
	}
	req.PlaceholderGroup = funcutil.SparseVectorDataToPlaceholderGroupBytes(idfSparseVector)
	return false, nil
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
