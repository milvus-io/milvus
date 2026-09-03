package idempotencyview

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func testInsert(timeTick uint64, pks []int64) *streamingpb.VChannelSummaryInsertRecord {
	record := &streamingpb.VChannelSummaryInsertRecord{
		SourceMessageId: &commonpb.MessageID{Id: "m"},
		SourceTimetick:  timeTick,
	}
	if len(pks) > 0 {
		record.Ids = &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
		}
	}
	return record
}

func TestRecordsFromSectionsJoinsByPosition(t *testing.T) {
	keys := []*streamingpb.VChannelSummaryIdempotencyRecord{
		{Key: "a", RowOffsets: []uint32{0}},
		{Key: "", RowOffsets: nil},
	}
	inserts := []*streamingpb.VChannelSummaryInsertRecord{
		testInsert(100, []int64{1}),
		testInsert(101, []int64{2}),
	}
	records, err := RecordsFromSections(keys, inserts)
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, "a", records[0].IdempotencyKey)
	assert.Equal(t, []uint32{0}, records[0].InsertResult.GetRowOffsets())
	// A write with no key still carries its primary keys: it is a write the
	// view knows about but has no client key to answer a duplicate for.
	assert.Empty(t, records[1].IdempotencyKey)
	assert.Equal(t, []int64{2}, records[1].InsertResult.GetIds().GetIntId().GetData())
}

func TestRecordsFromSectionsWithoutKeysSection(t *testing.T) {
	records, err := RecordsFromSections(nil, []*streamingpb.VChannelSummaryInsertRecord{testInsert(100, []int64{1})})
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Empty(t, records[0].IdempotencyKey)
	assert.Equal(t, []int64{1}, records[0].InsertResult.GetIds().GetIntId().GetData())
}

func TestRecordsFromSectionsRejectsMisalignment(t *testing.T) {
	// Joining a shorter key slice by position would attach a client key to
	// another write's primary keys, and a duplicate would then be answered with
	// rows it never inserted. Reject rather than truncate.
	_, err := RecordsFromSections(
		[]*streamingpb.VChannelSummaryIdempotencyRecord{{Key: "a"}},
		[]*streamingpb.VChannelSummaryInsertRecord{testInsert(100, []int64{1}), testInsert(101, []int64{2})},
	)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestRecordsFromSectionsLeavesEmptyResultNil(t *testing.T) {
	// Nothing to replay: no primary keys and no row offsets. The result must
	// stay nil rather than become an empty message that reads as an answer.
	records, err := RecordsFromSections(nil, []*streamingpb.VChannelSummaryInsertRecord{testInsert(100, nil)})
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Nil(t, records[0].InsertResult)
}
