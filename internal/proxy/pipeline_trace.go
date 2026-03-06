package proxy

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Msg keys used by pipeline nodes for output data.
const (
	reducedMsgKey         = "reduced"
	rankResultMsgKey      = "rank_result"
	fieldsMsgKey          = "fields"
	organizedFieldsMsgKey = "organized_fields"
)

// PipelineTrace collects diagnostic key-value entries during pipeline
// execution. All methods are nil-safe so callers never need nil checks.
// Not goroutine-safe; used only within the serial pipeline.Run loop.
type PipelineTrace struct {
	entries []traceEntry
}

type traceEntry struct {
	key string
	val any
}

// newPipelineTrace returns a new trace when enabled, nil otherwise.
func newPipelineTrace(enabled bool) *PipelineTrace {
	if !enabled {
		return nil
	}
	return &PipelineTrace{}
}

// Set appends a key-value pair. No-op on nil receiver.
func (t *PipelineTrace) Set(key string, val any) {
	if t == nil {
		return
	}
	t.entries = append(t.entries, traceEntry{key, val})
}

// TraceMsg inspects the pipeline msg after a node and records critical
// variable states. This is the single entry point called from pipeline.Run.
func (t *PipelineTrace) TraceMsg(opName string, msg opMsg) {
	if t == nil {
		return
	}
	switch opName {
	case searchReduceOp, hybridSearchReduceOp:
		t.traceReduce(opName, msg)
	case rerankOp:
		t.traceRerank(msg)
	case requeryOp:
		t.traceRequery(msg)
	case organizeOp:
		t.traceOrganize(msg)
	case endOp:
		t.traceEnd(msg)
	}
}

func (t *PipelineTrace) traceReduce(opName string, msg opMsg) {
	reduced, ok := msg[reducedMsgKey].([]*milvuspb.SearchResults)
	if !ok {
		return
	}
	for i, r := range reduced {
		prefix := fmt.Sprintf(opName+"[%d]", i)
		rd := r.GetResults()
		topks := rd.GetTopks()
		t.Set(prefix+".topks", topks)
		t.Set(prefix+".totalIDs", typeutil.GetSizeOfIDs(rd.GetIds()))
		if gbv := rd.GetGroupByFieldValue(); gbv != nil {
			t.Set(prefix+".groupByRows", fieldDataLen(gbv))
			t.Set(prefix+".groupByCards", scalarGroupByCards(gbv.GetScalars(), topks, gbv.GetValidData()))
		}
	}
}

func (t *PipelineTrace) traceRerank(msg opMsg) {
	t.traceSearchResult(rerankOp, msg, rankResultMsgKey)
}

func (t *PipelineTrace) traceEnd(msg opMsg) {
	t.traceSearchResult(endOp, msg, pipelineOutput)
}

func (t *PipelineTrace) traceSearchResult(prefix string, msg opMsg, key string) {
	result, ok := msg[key].(*milvuspb.SearchResults)
	if !ok {
		return
	}
	rd := result.GetResults()
	topks := rd.GetTopks()
	t.Set(prefix+".topks", topks)
	t.Set(prefix+".totalIDs", typeutil.GetSizeOfIDs(rd.GetIds()))
	t.Set(prefix+".fields", len(rd.GetFieldsData()))
	if gbv := rd.GetGroupByFieldValue(); gbv != nil {
		t.Set(prefix+".groupByRows", fieldDataLen(gbv))
		t.Set(prefix+".groupByCards", scalarGroupByCards(gbv.GetScalars(), topks, gbv.GetValidData()))
	}
}

func (t *PipelineTrace) traceRequery(msg opMsg) {
	fields, ok := msg[fieldsMsgKey].([]*schemapb.FieldData)
	if !ok {
		return
	}
	t.Set(requeryOp+".fields", len(fields))
	if len(fields) > 0 {
		t.Set(requeryOp+".rows", fieldDataLen(fields[0]))
	}
}

func (t *PipelineTrace) traceOrganize(msg opMsg) {
	batches, ok := msg[organizedFieldsMsgKey].([][]*schemapb.FieldData)
	if !ok {
		return
	}
	for i, fs := range batches {
		rows := 0
		if len(fs) > 0 {
			rows = fieldDataLen(fs[0])
		}
		t.Set(fmt.Sprintf(organizeOp+"[%d]", i), fmt.Sprintf("fields=%d rows=%d", len(fs), rows))
	}
}

// LogIfEnabled outputs the collected trace as a single DEBUG log line.
func (t *PipelineTrace) LogIfEnabled(ctx context.Context, pipelineName string) {
	if t == nil {
		return
	}
	log.Ctx(ctx).Debug("PipelineTrace", zap.String("pipeline", pipelineName), zap.String("trace", t.String()))
}

// String formats all entries as a single log line.
func (t *PipelineTrace) String() string {
	if t == nil {
		return ""
	}
	var sb strings.Builder
	for i, e := range t.entries {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s=%v", e.key, e.val)
	}
	return sb.String()
}

// fieldDataLen returns the logical number of rows in a FieldData.
// For string-type nullable fields the data array may be compact (non-null values only),
// so we use len(ValidData) which always equals the logical row count.
func fieldDataLen(fd *schemapb.FieldData) int {
	if fd == nil {
		return 0
	}
	if vd := fd.GetValidData(); len(vd) > 0 {
		return len(vd)
	}
	switch fd.GetField().(type) {
	case *schemapb.FieldData_Scalars:
		return scalarLen(fd.GetScalars())
	case *schemapb.FieldData_Vectors:
		return vectorLen(fd.GetVectors())
	}
	return 0
}

// scalarGroupByCards computes per-nq group cardinality (distinct count).
// validData is the nullable bitmap from FieldData; for compact-encoded types
// (StringData) it is used to correctly split compact data by NQ boundaries.
func scalarGroupByCards(s *schemapb.ScalarField, topks []int64, validData []bool) []int {
	if s == nil {
		return nil
	}
	switch d := s.GetData().(type) {
	case *schemapb.ScalarField_LongData:
		return perNqCardinality(d.LongData.GetData(), topks)
	case *schemapb.ScalarField_StringData:
		// StringData uses compact encoding: only non-null values are stored.
		// Use validData to correctly determine NQ boundaries.
		return perNqCardinalityCompact(d.StringData.GetData(), topks, validData)
	case *schemapb.ScalarField_IntData:
		return perNqCardinality(d.IntData.GetData(), topks)
	case *schemapb.ScalarField_BoolData:
		return perNqCardinality(d.BoolData.GetData(), topks)
	case *schemapb.ScalarField_FloatData:
		return perNqCardinality(d.FloatData.GetData(), topks)
	case *schemapb.ScalarField_DoubleData:
		return perNqCardinality(d.DoubleData.GetData(), topks)
	}
	return nil
}

// perNqCardinality splits vals by topks and returns per-nq distinct counts.
func perNqCardinality[T comparable](vals []T, topks []int64) []int {
	cards := make([]int, len(topks))
	offset := 0
	for i, k := range topks {
		end := offset + int(k)
		if end > len(vals) {
			end = len(vals)
		}
		cards[i] = countDistinct(vals[offset:end])
		offset = end
	}
	return cards
}

// perNqCardinalityCompact handles compact-encoded data (e.g. StringData where
// only non-null values are stored). It uses validData to map logical NQ
// boundaries (from topks) to the correct positions in the compact data array.
func perNqCardinalityCompact[T comparable](compact []T, topks []int64, validData []bool) []int {
	if len(validData) == 0 {
		return perNqCardinality(compact, topks)
	}
	cards := make([]int, len(topks))
	logicalOffset := 0
	compactIdx := 0
	for i, k := range topks {
		end := logicalOffset + int(k)
		if end > len(validData) {
			end = len(validData)
		}
		var nqVals []T
		hasNull := false
		for j := logicalOffset; j < end; j++ {
			if validData[j] && compactIdx < len(compact) {
				nqVals = append(nqVals, compact[compactIdx])
				compactIdx++
			} else {
				hasNull = true
			}
		}
		cards[i] = countDistinct(nqVals)
		if hasNull {
			cards[i]++
		}
		logicalOffset = end
	}
	return cards
}

// countDistinct returns the number of distinct values in a slice.
func countDistinct[T comparable](vals []T) int {
	if len(vals) == 0 {
		return 0
	}
	seen := make(map[T]struct{}, len(vals))
	for _, v := range vals {
		seen[v] = struct{}{}
	}
	return len(seen)
}

func scalarLen(s *schemapb.ScalarField) int {
	if s == nil {
		return 0
	}
	switch s.GetData().(type) {
	case *schemapb.ScalarField_LongData:
		return len(s.GetLongData().GetData())
	case *schemapb.ScalarField_StringData:
		return len(s.GetStringData().GetData())
	case *schemapb.ScalarField_IntData:
		return len(s.GetIntData().GetData())
	case *schemapb.ScalarField_BoolData:
		return len(s.GetBoolData().GetData())
	case *schemapb.ScalarField_FloatData:
		return len(s.GetFloatData().GetData())
	case *schemapb.ScalarField_DoubleData:
		return len(s.GetDoubleData().GetData())
	}
	return 0
}

func vectorLen(v *schemapb.VectorField) int {
	if v == nil || v.GetDim() == 0 {
		return 0
	}
	dim := int(v.GetDim())
	switch v.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		return len(v.GetFloatVector().GetData()) / dim
	case *schemapb.VectorField_BinaryVector:
		return len(v.GetBinaryVector()) / (dim / 8)
	}
	return 0
}
