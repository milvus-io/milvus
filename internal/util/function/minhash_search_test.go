package function

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMinHashSearchPlaceholderValidation(t *testing.T) {
	for _, holder := range []*commonpb.PlaceholderGroup{
		{},
		{Placeholders: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_VarChar}}},
		{Placeholders: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_BinaryVector, Values: [][]byte{{1}}}}},
	} {
		data, err := proto.Marshal(holder)
		require.NoError(t, err)
		_, err = MinHashSearchPlaceholder(data, nil)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
	_, err := MinHashSearchPlaceholder([]byte{255}, nil)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	data, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_VarChar, Values: [][]byte{[]byte("text")}}}})
	require.NoError(t, err)
	for _, output := range [][]any{nil, {"wrong type"}, {&schemapb.FieldData{}}, {&schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{}}}}} {
		mockey.PatchConvey("bad runner output remains an internal function failure", t, func() {
			mockey.Mock((*MinHashFunctionRunner).BatchRun).Return(output, nil).Build()
			_, err := MinHashSearchPlaceholder(data, &MinHashFunctionRunner{})
			require.ErrorIs(t, err, merr.ErrFunctionFailed)
		})
	}
	mockey.PatchConvey("runner error retains its cause", t, func() {
		original := merr.WrapErrServiceInternalMsg("runner unavailable")
		mockey.Mock((*MinHashFunctionRunner).BatchRun).Return(nil, original).Build()
		_, err := MinHashSearchPlaceholder(data, &MinHashFunctionRunner{})
		require.ErrorIs(t, err, original)
	})
}

func TestMinHashSearchPlaceholderSuccess(t *testing.T) {
	data, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_VarChar, Values: [][]byte{[]byte("first"), []byte("second")},
	}}})
	require.NoError(t, err)
	patch := mockey.Mock((*MinHashFunctionRunner).BatchRun).To(func(_ *MinHashFunctionRunner, inputs ...any) ([]any, error) {
		require.Equal(t, []string{"first", "second"}, inputs[0])
		return []any{&schemapb.FieldData{Type: schemapb.DataType_BinaryVector, Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2}}},
		}}}, nil
	}).Build()
	defer patch.UnPatch()
	converted, err := MinHashSearchPlaceholder(data, &MinHashFunctionRunner{})
	require.NoError(t, err)
	got := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(converted, got))
	require.Len(t, got.Placeholders, 1)
	require.Equal(t, commonpb.PlaceholderType_BinaryVector, got.Placeholders[0].Type)
	require.Equal(t, [][]byte{{1}, {2}}, got.Placeholders[0].Values)
}
