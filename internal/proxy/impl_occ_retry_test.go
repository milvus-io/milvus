package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestComputeOCCRetryParams_NonPartialUpdate(t *testing.T) {
	paramtable.Init()
	attempts, backoff := computeOCCRetryParams(false)
	assert.Equal(t, 1, attempts, "non partial update must run exactly once")
	assert.Equal(t, occInitialBackoff, backoff)
}

func TestComputeOCCRetryParams_PartialUpdateUsesParamtable(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.PartialUpdateRetryOnConflict.Key, "5")
	pt.Save(pt.ProxyCfg.PartialUpdateRetryBackoffMs.Key, "20")
	defer pt.Reset(pt.ProxyCfg.PartialUpdateRetryOnConflict.Key)
	defer pt.Reset(pt.ProxyCfg.PartialUpdateRetryBackoffMs.Key)

	attempts, backoff := computeOCCRetryParams(true)
	assert.Equal(t, 6, attempts, "maxAttempts == retries+1")
	assert.Equal(t, 20*time.Millisecond, backoff)
}

func TestComputeOCCRetryParams_NegativeRetriesClamped(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.PartialUpdateRetryOnConflict.Key, "-3")
	defer pt.Reset(pt.ProxyCfg.PartialUpdateRetryOnConflict.Key)

	attempts, _ := computeOCCRetryParams(true)
	assert.Equal(t, 1, attempts, "negative retries clamp to zero -> single attempt")
}

func TestComputeOCCRetryParams_ZeroBackoffKeepsDefault(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.PartialUpdateRetryBackoffMs.Key, "0")
	defer pt.Reset(pt.ProxyCfg.PartialUpdateRetryBackoffMs.Key)

	_, backoff := computeOCCRetryParams(true)
	assert.Equal(t, occInitialBackoff, backoff,
		"non-positive backoff should fall back to default")
}

func TestNextOCCBackoff_Doubles(t *testing.T) {
	assert.Equal(t, 10*time.Millisecond, nextOCCBackoff(5*time.Millisecond))
	assert.Equal(t, 40*time.Millisecond, nextOCCBackoff(20*time.Millisecond))
}

func TestNextOCCBackoff_CapsAtMax(t *testing.T) {
	assert.Equal(t, occMaxBackoff, nextOCCBackoff(80*time.Millisecond),
		"80ms*2 == 160ms must clamp to 100ms")
	assert.Equal(t, occMaxBackoff, nextOCCBackoff(occMaxBackoff))
	assert.Equal(t, occMaxBackoff, nextOCCBackoff(200*time.Millisecond),
		"already-over-max input must stay at max")
}

func TestBuildUpsertOCCInput_NonPartialUpdate(t *testing.T) {
	pks := &schemapb.IDs{}
	got := buildUpsertOCCInput(false, pks, []uint64{1, 2}, []bool{true, false})
	assert.Nil(t, got, "non partial-update must produce no OCC metadata")
}

func TestBuildUpsertOCCInput_EmptyExpectedTs(t *testing.T) {
	pks := &schemapb.IDs{}
	got := buildUpsertOCCInput(true, pks, nil, nil)
	assert.Nil(t, got, "no expected-row info must produce no OCC metadata")
}

func TestBuildUpsertOCCInput_PopulatesAllFields(t *testing.T) {
	pks := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10, 11}}},
	}
	expectedTs := []uint64{100, 200}
	expectedExists := []bool{true, false}

	got := buildUpsertOCCInput(true, pks, expectedTs, expectedExists)

	assert.NotNil(t, got)
	assert.Same(t, pks, got.PKs)
	assert.Equal(t, expectedTs, got.ExpectedTs)
	assert.Equal(t, expectedExists, got.ExpectedExists)
}

func TestAttachOCCToInsertHeader_StampsAllFields(t *testing.T) {
	header := &message.InsertMessageHeader{}
	pks := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{42}}},
	}
	occ := &OCCRowMeta{
		PKs:            pks,
		ExpectedTs:     []uint64{1234},
		ExpectedExists: []bool{true},
	}

	attachOCCToInsertHeader(header, occ)

	assert.Equal(t, messagespb.OCCMode_OCC_MODE_CAS, header.OccMode)
	assert.Same(t, pks, header.ExpectedPks)
	assert.Equal(t, []uint64{1234}, header.ExpectedRowTimestamps)
	assert.Equal(t, []bool{true}, header.ExpectedRowExists)
}

// --- helpers extracted from queryPreExecute ---

func tsFieldData(values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: common.TimeStampField,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: values},
				},
			},
		},
	}
}

func intPKField(id int64, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: id,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: values},
				},
			},
		},
	}
}

func TestExtractHiddenTimestampColumn_Found(t *testing.T) {
	pk := intPKField(100, []int64{1, 2, 3})
	ts := tsFieldData([]int64{10, 20, 30})
	in := []*schemapb.FieldData{pk, ts}

	out, rowTs, ok := extractHiddenTimestampColumn(in)

	assert.True(t, ok)
	assert.Equal(t, []uint64{10, 20, 30}, rowTs)
	assert.Len(t, out, 1)
	assert.Equal(t, int64(100), out[0].GetFieldId())
}

func TestExtractHiddenTimestampColumn_TimestampFirst(t *testing.T) {
	ts := tsFieldData([]int64{7})
	pk := intPKField(100, []int64{42})
	in := []*schemapb.FieldData{ts, pk}

	out, rowTs, ok := extractHiddenTimestampColumn(in)

	assert.True(t, ok)
	assert.Equal(t, []uint64{7}, rowTs)
	assert.Len(t, out, 1)
	assert.Equal(t, int64(100), out[0].GetFieldId())
}

func TestExtractHiddenTimestampColumn_Missing(t *testing.T) {
	pk := intPKField(100, []int64{1, 2})
	in := []*schemapb.FieldData{pk}

	out, rowTs, ok := extractHiddenTimestampColumn(in)

	assert.False(t, ok, "missing ts column must be reported as not-found")
	assert.Nil(t, rowTs)
	assert.Len(t, out, 1, "input slice returned unchanged")
}

func TestExtractHiddenTimestampColumn_Empty(t *testing.T) {
	out, rowTs, ok := extractHiddenTimestampColumn(nil)
	assert.False(t, ok)
	assert.Nil(t, rowTs)
	assert.Empty(t, out)
}

func TestAssignOCCRowVersions_NoRows(t *testing.T) {
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: nil}}}
	occTs, occExists, err := assignOCCRowVersions(ids, nil, nil, map[interface{}]int{}, nil)
	assert.NoError(t, err)
	assert.Nil(t, occTs)
	assert.Nil(t, occExists)
}

func TestAssignOCCRowVersions_UpdatesAndInserts(t *testing.T) {
	// upsert payload PKs: [1, 2, 3]
	upsertIDs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}
	// update indices reference PKs at upsertIDs[0], upsertIDs[2] (i.e. 1 and 3)
	updateIdx := []int{0, 2}
	insertIdx := []int{1}
	// existing rows: PK 1 -> ts 100 (existIndex 0), PK 3 -> ts 300 (existIndex 1)
	existPKToIndex := map[interface{}]int{int64(1): 0, int64(3): 1}
	existRowTs := []uint64{100, 300}

	occTs, occExists, err := assignOCCRowVersions(upsertIDs, updateIdx, insertIdx, existPKToIndex, existRowTs)

	assert.NoError(t, err)
	// layout: updates first (in updateIdx order), then first-writes
	assert.Equal(t, []uint64{100, 300, 0}, occTs)
	assert.Equal(t, []bool{true, true, false}, occExists)
}

func TestAssignOCCRowVersions_MissingPKMappingReturnsError(t *testing.T) {
	upsertIDs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{42}}}}
	updateIdx := []int{0}
	// mapping intentionally empty: simulates internal invariant violation.
	existPKToIndex := map[interface{}]int{}

	occTs, occExists, err := assignOCCRowVersions(upsertIDs, updateIdx, nil, existPKToIndex, []uint64{})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid),
		"expected ParameterInvalid for missing pk mapping, got: %v", err)
	assert.Nil(t, occTs)
	assert.Nil(t, occExists)
}

func TestAssignOCCRowVersions_NilExistRowTsDegrades(t *testing.T) {
	upsertIDs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
	updateIdx := []int{0}
	existPKToIndex := map[interface{}]int{int64(1): 0}

	// existRowTs == nil simulates the missing-ts-column degraded path.
	occTs, occExists, err := assignOCCRowVersions(upsertIDs, updateIdx, nil, existPKToIndex, nil)

	assert.NoError(t, err)
	assert.Equal(t, []uint64{0}, occTs, "missing existRowTs => 0 (first-write semantics)")
	assert.Equal(t, []bool{true}, occExists, "still tagged as expected-exists for the update branch")
}

func TestAssignOCCRowVersions_ExistIndexOutOfRangeKeepsZero(t *testing.T) {
	// Defensive bound check: even if mapping points to an existIndex >= len(existRowTs)
	// (shouldn't happen but the helper guards it), assignment must not panic
	// and must default the row timestamp to 0.
	upsertIDs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
	updateIdx := []int{0}
	existPKToIndex := map[interface{}]int{int64(1): 5}
	existRowTs := []uint64{100, 200}

	occTs, occExists, err := assignOCCRowVersions(upsertIDs, updateIdx, nil, existPKToIndex, existRowTs)

	assert.NoError(t, err)
	assert.Equal(t, []uint64{0}, occTs)
	assert.Equal(t, []bool{true}, occExists)
}

// --- classifyOCCRetry: pure decision function for the retry loop ---

func newPKStateConflictErr() error {
	return status.NewInner("%sversion mismatch", status.PKStateConflictCausePrefix)
}

func TestClassifyOCCRetry_NilErrIsTerminal(t *testing.T) {
	retry, exhausted := classifyOCCRetry(nil, true, 0, 3)
	assert.False(t, retry)
	assert.False(t, exhausted)
}

func TestClassifyOCCRetry_NonPartialUpdateNeverRetries(t *testing.T) {
	retry, exhausted := classifyOCCRetry(newPKStateConflictErr(), false, 0, 3)
	assert.False(t, retry, "non-partial-update upsert must never trigger OCC retry")
	assert.False(t, exhausted)
}

func TestClassifyOCCRetry_NonCASErrorNeverRetries(t *testing.T) {
	retry, exhausted := classifyOCCRetry(errors.New("some other error"), true, 0, 3)
	assert.False(t, retry)
	assert.False(t, exhausted)
}

func TestClassifyOCCRetry_CASConflictRetriesUntilLast(t *testing.T) {
	retry, exhausted := classifyOCCRetry(newPKStateConflictErr(), true, 0, 3)
	assert.True(t, retry)
	assert.False(t, exhausted)
}

func TestClassifyOCCRetry_CASConflictExhaustedOnLastAttempt(t *testing.T) {
	retry, exhausted := classifyOCCRetry(newPKStateConflictErr(), true, 2, 3)
	assert.False(t, retry, "must not retry after the final attempt")
	assert.True(t, exhausted)
}

func TestClassifyOCCRetry_SingleAttemptExhaustsImmediately(t *testing.T) {
	// maxAttempts=1 means the first failure already exhausts retries.
	retry, exhausted := classifyOCCRetry(newPKStateConflictErr(), true, 0, 1)
	assert.False(t, retry)
	assert.True(t, exhausted)
}

// --- newUpsertTaskForRetry ---

func TestNewUpsertTaskForRetry_InitializesAllFields(t *testing.T) {
	ctx := context.Background()
	req := &milvuspb.UpsertRequest{
		HashKeys:        []uint32{1, 2, 3},
		SchemaTimestamp: 9999,
	}
	node := &Proxy{}

	it := newUpsertTaskForRetry(ctx, req, node)

	assert.NotNil(t, it)
	assert.Equal(t, ctx, it.ctx)
	assert.Same(t, req, it.req)
	assert.Same(t, node, it.node)
	assert.Equal(t, []uint32{1, 2, 3}, it.baseMsg.HashValues)
	assert.Equal(t, uint64(9999), it.schemaTimestamp)
	assert.NotNil(t, it.result, "fresh task must have an initialized result")
	assert.NotNil(t, it.result.IDs)
	assert.NotNil(t, it.Condition, "Condition must be reset so a retry can wait for completion")
}
