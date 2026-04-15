package reduce

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestFindFieldDataByID(t *testing.T) {
	field := int64FieldData(101, "brand", []int64{1})
	fields := []*schemapb.FieldData{int64FieldData(100, "id", []int64{10}), field}

	require.Same(t, field, FindFieldDataByID(fields, 101))
	require.Nil(t, FindFieldDataByID(fields, 102))
	require.Nil(t, FindFieldDataByID(nil, 101))
}

func TestFindGroupByFieldData(t *testing.T) {
	plural := int64FieldData(101, "brand", []int64{1})
	singular := int64FieldData(0, "legacy", []int64{2})
	data := &schemapb.SearchResultData{
		GroupByFieldValues: []*schemapb.FieldData{plural},
		GroupByFieldValue:  singular,
	}

	require.Same(t, plural, FindGroupByFieldData(data, 101, false))
	require.Nil(t, FindGroupByFieldData(data, 102, false))
	require.Same(t, singular, FindGroupByFieldData(data, 102, true))
	require.Nil(t, FindGroupByFieldData(nil, 101, true))
}

func TestValidateGroupByFieldsPresent(t *testing.T) {
	results := []*schemapb.SearchResultData{
		nil,
		{Ids: intIDs()},
		{
			Ids:                intIDs(1),
			GroupByFieldValues: []*schemapb.FieldData{int64FieldData(101, "brand", []int64{10})},
		},
	}

	require.NoError(t, ValidateGroupByFieldsPresent(results, []int64{101}, false))
	err := ValidateGroupByFieldsPresent(results, []int64{102}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "group-by field 102 missing from search result 2")

	legacyResults := []*schemapb.SearchResultData{{
		Ids:               intIDs(1),
		GroupByFieldValue: int64FieldData(0, "legacy", []int64{10}),
	}}
	require.NoError(t, ValidateGroupByFieldsPresent(legacyResults, []int64{101}, true))
}

func TestWriteGroupByFieldValuesEmptyAcceptedRows(t *testing.T) {
	ret := &schemapb.SearchResultData{}
	err := WriteGroupByFieldValues(ret, nil, []*schemapb.SearchResultData{{}}, []int64{101})
	require.NoError(t, err)
	require.Empty(t, ret.GetGroupByFieldValues())
}

func TestWriteGroupByFieldValuesPreservesFieldNameAndFieldID(t *testing.T) {
	sources := []*schemapb.SearchResultData{
		{GroupByFieldValues: []*schemapb.FieldData{
			int64FieldData(101, "brand", []int64{10, 11}),
			int64FieldData(102, "category", []int64{20, 21}),
		}},
		{GroupByFieldValues: []*schemapb.FieldData{
			int64FieldData(101, "brand", []int64{12}),
			int64FieldData(102, "category", []int64{22}),
		}},
	}
	ret := &schemapb.SearchResultData{}

	err := WriteGroupByFieldValues(ret, []RowRef{{ResultIdx: 0, RowIdx: 1}, {ResultIdx: 1, RowIdx: 0}}, sources, []int64{101, 102})

	require.NoError(t, err)
	require.Len(t, ret.GetGroupByFieldValues(), 2)
	require.Equal(t, int64(101), ret.GetGroupByFieldValues()[0].GetFieldId())
	require.Equal(t, "brand", ret.GetGroupByFieldValues()[0].GetFieldName())
	require.Equal(t, []int64{11, 12}, ret.GetGroupByFieldValues()[0].GetScalars().GetLongData().GetData())
	require.Equal(t, int64(102), ret.GetGroupByFieldValues()[1].GetFieldId())
	require.Equal(t, "category", ret.GetGroupByFieldValues()[1].GetFieldName())
	require.Equal(t, []int64{21, 22}, ret.GetGroupByFieldValues()[1].GetScalars().GetLongData().GetData())
}

func TestWriteGroupByFieldValuesLegacySingularFallback(t *testing.T) {
	ret := &schemapb.SearchResultData{}
	err := WriteGroupByFieldValues(ret, []RowRef{{ResultIdx: 0, RowIdx: 1}}, []*schemapb.SearchResultData{{
		GroupByFieldValue: int64FieldData(0, "legacy_brand", []int64{10, 11}),
	}}, []int64{101})

	require.NoError(t, err)
	require.Len(t, ret.GetGroupByFieldValues(), 1)
	require.Equal(t, int64(101), ret.GetGroupByFieldValues()[0].GetFieldId())
	require.Equal(t, "legacy_brand", ret.GetGroupByFieldValues()[0].GetFieldName())
	require.Equal(t, []int64{11}, ret.GetGroupByFieldValues()[0].GetScalars().GetLongData().GetData())
}

func TestWriteGroupByFieldValuesMissingSourceErrors(t *testing.T) {
	t.Run("missing from all sources", func(t *testing.T) {
		err := WriteGroupByFieldValues(&schemapb.SearchResultData{}, []RowRef{{ResultIdx: 0}}, []*schemapb.SearchResultData{{}}, []int64{101, 102})
		require.Error(t, err)
		require.Contains(t, err.Error(), "group-by field 101 missing from all source shards")
	})

	t.Run("missing from accepted source", func(t *testing.T) {
		err := WriteGroupByFieldValues(&schemapb.SearchResultData{}, []RowRef{{ResultIdx: 1}}, []*schemapb.SearchResultData{
			{GroupByFieldValues: []*schemapb.FieldData{int64FieldData(101, "brand", []int64{10})}},
			{},
		}, []int64{101})
		require.Error(t, err)
		require.Contains(t, err.Error(), "group-by field 101 missing at source shard index 1")
	})
}

func int64FieldData(fieldID int64, fieldName string, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   fieldID,
		FieldName: fieldName,
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
}

func intIDs(values ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: values}}}
}
