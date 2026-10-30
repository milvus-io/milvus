package dql

import (
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const CalcDistanceKey = "calc_distance"

// calcDistanceReservedFieldID is the FieldId used by the QueryNode for the
// synthetic distance column (see internal/querynodev2/tasks/calc_distance.go
// reservedFieldID). It must stay in sync: the proxy injects it into the ORDER
// BY remap target so the column survives the FieldsData rebuild.
const calcDistanceReservedFieldID int64 = -10000

// calcDistanceSpec is the resolved calc_distance request: the reference vector,
// the target vector field, and the metric. It is written into the query plan so
// the QueryNode can compute per-row distances during reduce.
type calcDistanceSpec struct {
	// refVector is the single reference vector in its canonical byte encoding
	// for refVectorType (float32 LE bytes for FloatVector, half-precision bytes
	// for Float16/BFloat16, raw packed bits for BinaryVector, dim bytes for
	// Int8Vector, compact sparse-row bytes for SparseFloatVector). It is
	// produced in resolveCalcDistance from either vectorInput (array/object
	// form) or the expression form's extracted row.
	refVector []byte
	// refVectorType is the target vector field's data type. It drives both how
	// refVector is encoded (proxy) and how the QueryNode decodes it, so every
	// supported vector field type can be a calc_distance target.
	refVectorType schemapb.DataType
	// vectorInput is the raw JSON of the direct-vector input form (array for
	// dense types, object/array-of-pairs for sparse), kept until the schema is
	// known so it can be interpreted per refVectorType. Empty for the
	// expression form.
	vectorInput json.RawMessage
	// expression is the single-vector expression string form, empty for the
	// direct-vector form. It is resolved (and cleared) in resolveCalcDistance.
	expression string
	// metricType is the distance metric, upper-cased, validated against the
	// vector type's supported metric set.
	metricType string
	// fieldName is the target vector field name requested by the user.
	fieldName string
	// vectorField is the target vector field, validated to be a supported
	// vector field type.
	vectorField *schemapb.FieldSchema
	// distanceOutputName is the resolved output column name (default
	// "distance", prefixed with '_' until free of collision).
	distanceOutputName string
}

// parseCalcDistance reads the calc_distance key from query params and parses it
// into a spec skeleton holding the raw reference input (array or expression
// string) and the requested metric/field names. Schema-dependent validation and
// expression resolution happen in resolveCalcDistance.
func parseCalcDistance(queryParamsPair []*commonpb.KeyValuePair) (*calcDistanceSpec, error) {
	raw, err := funcutil.GetAttrByKeyFromRepeatedKV(CalcDistanceKey, queryParamsPair)
	if err != nil {
		// key absent: no calc_distance requested
		return nil, nil
	}

	if !gjson.Valid(raw) {
		return nil, merr.WrapErrParameterInvalidMsg("%s must be a valid JSON object", CalcDistanceKey)
	}

	var request struct {
		MetricType string          `json:"metric_type"`
		Field      string          `json:"field"`
		Vector     json.RawMessage `json:"vector"`
	}
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("%s must be a JSON object: %v", CalcDistanceKey, err)
	}
	if request.MetricType == "" {
		return nil, merr.WrapErrParameterInvalidMsg("%s requires metric_type", CalcDistanceKey)
	}
	if request.Field == "" {
		return nil, merr.WrapErrParameterInvalidMsg("%s requires field", CalcDistanceKey)
	}
	if len(request.Vector) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("%s requires vector", CalcDistanceKey)
	}

	spec := &calcDistanceSpec{
		metricType: strings.ToUpper(request.MetricType),
		fieldName:  request.Field,
	}
	// The spec carries the target field name for schema resolution later.
	// refVector or the expression string are both stored on the spec below.

	// vector is either a JSON array/object (direct vector, interpretation
	// depends on the target field's data type) or a JSON string (single-vector
	// expression). Distinguish by the first byte.
	switch request.Vector[0] {
	case '[', '{':
		// direct vector; kept raw and interpreted in resolveCalcDistance once
		// the target field's data type is known
		spec.vectorInput = request.Vector
	case '"':
		// expression string, resolved later
		var expr string
		if err := json.Unmarshal(request.Vector, &expr); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector expression parse failed: %v", CalcDistanceKey, err)
		}
		spec.expression = expr
	default:
		return nil, merr.WrapErrParameterInvalidMsg("%s.vector must be a vector array/object or a single-vector expression string", CalcDistanceKey)
	}

	return spec, nil
}

// resolveCalcDistance validates the spec against the collection schema and, for
// the expression input form, resolves the reference vector by running an
// internal query. It must run after the schema is known.
func (t *QueryTask) resolveCalcDistance(spec *calcDistanceSpec, ctx context.Context, span trace.Span) error {
	// find and validate the target vector field
	var vectorField *schemapb.FieldSchema
	for _, field := range t.schema.CollectionSchema.GetFields() {
		if field.GetName() == spec.fieldName {
			vectorField = field
			break
		}
	}
	if vectorField == nil {
		return merr.WrapErrParameterInvalidMsg("%s.field %s does not exist", CalcDistanceKey, spec.fieldName)
	}
	if !isSupportedCalcDistanceVectorType(vectorField.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("%s.field %s has unsupported vector type %s",
			CalcDistanceKey, spec.fieldName, vectorField.GetDataType().String())
	}
	// sparse vectors carry their own dimension per row; only fixed-dimension
	// dense types have a schema dim to validate the reference against
	var dim int64
	if vectorField.GetDataType() != schemapb.DataType_SparseFloatVector {
		dim, err = typeutil.GetDim(vectorField)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("%s.field %s dimension resolution failed: %v", CalcDistanceKey, spec.fieldName, err)
		}
		if dim <= 0 {
			return merr.WrapErrParameterInvalidMsg("%s.field %s has invalid dimension %d", CalcDistanceKey, spec.fieldName, dim)
		}
	}

	metricType, err := validateMetricTypeForVector(spec.metricType, vectorField.GetDataType())
	if err != nil {
		return err
	}
	spec.metricType = metricType
	spec.vectorField = vectorField
	spec.refVectorType = vectorField.GetDataType()

	// resolve reference vector: expression form runs an internal query
	if spec.expression != "" {
		refVec, err := t.resolveExpressionVector(spec.expression, spec.fieldName, dim, ctx, span)
		if err != nil {
			return err
		}
		spec.refVector = refVec
		spec.expression = ""
	} else {
		refVec, err := encodeCalcDistanceVector(spec.vectorInput, spec.refVectorType, dim)
		if err != nil {
			return err
		}
		spec.refVector = refVec
	}
	// validate the byte length against the expected encoding of the field type
	if expected, fixed := expectedRefVectorBytesLen(spec.refVectorType, dim); fixed && int64(len(spec.refVector)) != expected {
		return merr.WrapErrParameterInvalidMsg("%s.vector dimension does not match field %s: got %d bytes, want %d",
			CalcDistanceKey, spec.fieldName, len(spec.refVector), expected)
	}
	if len(spec.refVector) == 0 {
		return merr.WrapErrParameterInvalidMsg("%s.vector must not be an empty vector", CalcDistanceKey)
	}
	// a zero-norm dense reference vector would produce NaN for every row under
	// COSINE (CosineImplPure divides by sqrt(normA*normB)); reject it up front
	// so the caller gets an explicit error instead of a silent all-NaN column
	if spec.metricType == metric.COSINE {
		if err := validateCosineRefVector(spec.refVector, spec.refVectorType); err != nil {
			return err
		}
	}
	return nil
}

// validateCosineRefVector rejects a zero-norm reference vector for COSINE.
// Sparse rows already return 0 for zero norms (SparseCosineImpl) and binary
// vectors are metric-restricted to HAMMING/JACCARD, so only dense float-like
// types need the check.
func validateCosineRefVector(refVector []byte, dataType schemapb.DataType) error {
	switch dataType {
	case schemapb.DataType_FloatVector:
		for _, v := range typeutil.BytesToFloat32Vector(refVector) {
			if v != 0 {
				return nil
			}
		}
	case schemapb.DataType_Int8Vector:
		for _, b := range refVector {
			if int8(b) != 0 {
				return nil
			}
		}
	case schemapb.DataType_Float16Vector:
		for _, v := range typeutil.Float16BytesToFloat32Vector(refVector) {
			if v != 0 {
				return nil
			}
		}
	case schemapb.DataType_BFloat16Vector:
		for _, v := range typeutil.BFloat16BytesToFloat32Vector(refVector) {
			if v != 0 {
				return nil
			}
		}
	default:
		// sparse / binary: handled by their own metric semantics
		return nil
	}
	return merr.WrapErrParameterInvalidMsg("%s.vector has zero norm, which is not allowed with metric_type COSINE", CalcDistanceKey)
}

// isSupportedCalcDistanceVectorType reports whether the vector field type can
// be a calc_distance target.
func isSupportedCalcDistanceVectorType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
		schemapb.DataType_SparseFloatVector:
		return true
	}
	return false
}

// expectedRefVectorBytesLen returns the byte length of the canonical reference
// vector encoding for a fixed-dimension field type. The bool is false for
// variable-dimension types (sparse), where no length check applies.
func expectedRefVectorBytesLen(dataType schemapb.DataType, dim int64) (int64, bool) {
	switch dataType {
	case schemapb.DataType_FloatVector:
		return dim * 4, true
	case schemapb.DataType_BinaryVector:
		return dim / 8, true
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return dim * 2, true
	case schemapb.DataType_Int8Vector:
		return dim, true
	case schemapb.DataType_SparseFloatVector:
		return 0, false
	}
	return 0, false
}

// encodeCalcDistanceVector interprets the raw JSON direct-vector input according
// to the target field's data type and returns the canonical byte encoding stored
// in the plan and consumed by the QueryNode.
func encodeCalcDistanceVector(input json.RawMessage, dataType schemapb.DataType, dim int64) ([]byte, error) {
	switch dataType {
	case schemapb.DataType_FloatVector:
		var data []float32
		if err := json.Unmarshal(input, &data); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector must be a JSON array of numbers: %v", CalcDistanceKey, err)
		}
		if len(data) != int(dim) {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector dimension %d does not match field dimension %d", CalcDistanceKey, len(data), dim)
		}
		return typeutil.Float32ArrayToBytes(data), nil
	case schemapb.DataType_Int8Vector:
		var data []int8
		if err := json.Unmarshal(input, &data); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector must be a JSON array of integers for an int8 vector field: %v", CalcDistanceKey, err)
		}
		if len(data) != int(dim) {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector dimension %d does not match field dimension %d", CalcDistanceKey, len(data), dim)
		}
		return typeutil.Int8ArrayToBytes(data), nil
	case schemapb.DataType_Float16Vector:
		var data []float32
		if err := json.Unmarshal(input, &data); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector must be a JSON array of numbers for a float16 vector field: %v", CalcDistanceKey, err)
		}
		if len(data) != int(dim) {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector dimension %d does not match field dimension %d", CalcDistanceKey, len(data), dim)
		}
		return typeutil.Float32ArrayToFloat16Bytes(data), nil
	case schemapb.DataType_BFloat16Vector:
		var data []float32
		if err := json.Unmarshal(input, &data); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector must be a JSON array of numbers for a bfloat16 vector field: %v", CalcDistanceKey, err)
		}
		if len(data) != int(dim) {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector dimension %d does not match field dimension %d", CalcDistanceKey, len(data), dim)
		}
		return typeutil.Float32ArrayToBFloat16Bytes(data), nil
	case schemapb.DataType_BinaryVector:
		if dim%8 != 0 {
			return nil, merr.WrapErrParameterInvalidMsg("%s.field dimension %d is not a multiple of 8 for a binary vector field", CalcDistanceKey, dim)
		}
		var data []int
		if err := json.Unmarshal(input, &data); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector must be a JSON array of byte values (0-255) for a binary vector field: %v", CalcDistanceKey, err)
		}
		if len(data) != int(dim/8) {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector byte count %d does not match field byte count %d", CalcDistanceKey, len(data), dim/8)
		}
		ref := make([]byte, len(data))
		for i, v := range data {
			if v < 0 || v > 255 {
				return nil, merr.WrapErrParameterInvalidMsg("%s.vector element %d is out of range [0, 255] for a binary vector field", CalcDistanceKey, i)
			}
			ref[i] = byte(v)
		}
		return ref, nil
	case schemapb.DataType_SparseFloatVector:
		if len(input) > 0 && input[0] == '{' {
			row, err := typeutil.CreateSparseFloatRowFromJSON(input)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("%s.vector must be a sparse vector JSON object ({\"indices\":[...],\"values\":[...]} or {\"1\":0.1,...}): %v", CalcDistanceKey, err)
			}
			return row, nil
		}
		var pairs []struct {
			Index uint32  `json:"index"`
			Value float32 `json:"value"`
		}
		if err := json.Unmarshal(input, &pairs); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector must be a JSON array of {\"index\":...,\"value\":...} pairs for a sparse vector field: %v", CalcDistanceKey, err)
		}
		indices := make([]uint32, len(pairs))
		values := make([]float32, len(pairs))
		for i, p := range pairs {
			indices[i] = p.Index
			values[i] = p.Value
		}
		indices, values = typeutil.SortSparseFloatRow(indices, values)
		row := typeutil.CreateSparseFloatRow(indices, values)
		if err := typeutil.ValidateSparseFloatRows(row); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("%s.vector is not a valid sparse vector: %v", CalcDistanceKey, err)
		}
		return row, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("%s.field has unsupported vector type %s", CalcDistanceKey, dataType.String())
	}
}

// resolveExpressionVector runs an internal query with the given expression and
// output field, returning exactly the single matching vector in its canonical
// byte encoding for the field's data type. It errors when the expression matches
// zero or more than one row. The internal query reuses the normal query path:
// the plan is built from request.Expr by createPlanArgs, and limit=2 is carried
// through query_params so segcore stops early.
func (t *QueryTask) resolveExpressionVector(expr string, outputField string, dim int64, ctx context.Context, span trace.Span) ([]byte, error) {
	fieldID := t.getFieldID(outputField)
	queryReq := &milvuspb.QueryRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Retrieve,
			Timestamp: t.BeginTs(),
		},
		DbName:                t.request.GetDbName(),
		CollectionName:        t.request.GetCollectionName(),
		ConsistencyLevel:      t.request.GetConsistencyLevel(),
		NotReturnAllMeta:      t.request.GetNotReturnAllMeta(),
		Expr:                  expr,
		OutputFields:          []string{outputField},
		PartitionNames:        t.request.GetPartitionNames(),
		UseDefaultConsistency: t.request.GetUseDefaultConsistency(),
		GuaranteeTimestamp:    t.request.GetGuaranteeTimestamp(),
		Namespace:             t.request.Namespace,
		QueryParams: []*commonpb.KeyValuePair{
			{Key: LimitKey, Value: "2"},
		},
	}

	qt := &QueryTask{
		baseTask: baseTask{
			MetaCache: t.GetMetaCache(),
		},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID:            paramtable.GetNodeID(),
			PartitionIDs:     t.PartitionIDs,
			ConsistencyLevel: t.ConsistencyLevel,
			QueryLabel:       metrics.QueryLabel,
		},
		request:        queryReq,
		lb:             t.lb,
		shardclientMgr: t.shardclientMgr,
		channelsMvcc:   t.channelsMvcc,
		preferredNodes: t.preferredNodes,
		fastSkip:       true,
		reQuery:        true,
		node:           t.node,
		internalTask:   true,
		chMgr:          t.chMgr,
	}
	queryRunner, ok := t.node.(taskmodel.QueryRunner)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("host node does not implement QueryRunner")
	}
	queryResult, _, err := queryRunner.ExecuteQuery(ctx, qt, span)
	if err != nil {
		return nil, err
	}
	if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, merr.Error(queryResult.GetStatus())
	}
	if typeutil.GetSizeOfIDs(queryResult.GetIds()) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("%s.vector expression must match exactly one vector", CalcDistanceKey)
	}
	if typeutil.GetSizeOfIDs(queryResult.GetIds()) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("%s.vector expression matched no rows", CalcDistanceKey)
	}
	// the query returns exactly the requested vector field as the first field
	var vec []byte
	var fieldType schemapb.DataType
	found := false
	for _, fd := range queryResult.GetFieldsData() {
		if fd.GetFieldId() == fieldID {
			found = true
			fieldType = fd.GetType()
			switch fieldType {
			case schemapb.DataType_FloatVector:
				vec = typeutil.Float32ArrayToBytes(fd.GetVectors().GetFloatVector().GetData())
			case schemapb.DataType_BinaryVector:
				vec = fd.GetVectors().GetBinaryVector()
			case schemapb.DataType_Float16Vector:
				vec = fd.GetVectors().GetFloat16Vector()
			case schemapb.DataType_BFloat16Vector:
				vec = fd.GetVectors().GetBfloat16Vector()
			case schemapb.DataType_Int8Vector:
				vec = fd.GetVectors().GetInt8Vector()
			case schemapb.DataType_SparseFloatVector:
				contents := fd.GetVectors().GetSparseFloatVector().GetContents()
				if len(contents) != 1 {
					return nil, merr.WrapErrParameterInvalidMsg("%s.vector expression must resolve to a single sparse vector row", CalcDistanceKey)
				}
				vec = contents[0]
			}
			break
		}
	}
	if !found {
		return nil, merr.WrapErrServiceInternalMsg("%s.vector expression query did not return the vector field %s", CalcDistanceKey, outputField)
	}
	// reuse the same fixed-dimension length check as the direct-vector path
	if expected, fixed := expectedRefVectorBytesLen(fieldType, dim); fixed && int64(len(vec)) != expected {
		return nil, merr.WrapErrParameterInvalidMsg("%s.vector expression resolved vector dimension does not match field %s dimension %d",
			CalcDistanceKey, outputField, dim)
	}
	return vec, nil
}

// getFieldID returns the field id of the named field, or 0.
func (t *QueryTask) getFieldID(fieldName string) int64 {
	for _, field := range t.schema.CollectionSchema.GetFields() {
		if field.GetName() == fieldName {
			return field.GetFieldID()
		}
	}
	return 0
}

// validateMetricTypeForVector returns the upper-cased metric if supported for
// the given vector field type, else an error. The supported set follows the
// brute-force computable metrics of each vector family.
func validateMetricTypeForVector(metricType string, dataType schemapb.DataType) (string, error) {
	var supported []string
	switch dataType {
	case schemapb.DataType_BinaryVector:
		supported = []string{metric.HAMMING, metric.JACCARD}
	case schemapb.DataType_SparseFloatVector:
		supported = []string{metric.IP, metric.COSINE}
	default:
		// dense float-like families: FloatVector, Float16, BFloat16, Int8
		supported = []string{metric.L2, metric.IP, metric.COSINE}
	}
	m := strings.ToUpper(metricType)
	for _, s := range supported {
		if m == s {
			return m, nil
		}
	}
	return "", merr.WrapErrParameterInvalidMsg("%s metric_type %s is not supported for vector field type %s, only %v for now",
		CalcDistanceKey, metricType, dataType.String(), supported)
}
