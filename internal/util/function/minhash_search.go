package function

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MinHashSearchPlaceholder converts text input with the caller's pinned runner.
func MinHashSearchPlaceholder(placeholder []byte, functionRunner FunctionRunner) ([]byte, error) {
	pb := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(placeholder, pb); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "failed to unmarshal MinHash placeholder group")
	}

	if len(pb.Placeholders) != 1 || len(pb.Placeholders[0].Values) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("please provide varchar/text for MinHash Function based search")
	}

	holder := pb.Placeholders[0]
	if holder.Type != commonpb.PlaceholderType_VarChar {
		return nil, merr.WrapErrParameterInvalidMsg("please provide varchar/text for MinHash Function based search, got %s", holder.Type.String())
	}

	texts := funcutil.GetVarCharFromPlaceholder(holder)
	output, err := functionRunner.BatchRun(texts)
	if err != nil {
		return nil, err
	}
	if len(output) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("MinHash embedding failed: runner returned empty output")
	}

	fieldData, ok := output[0].(*schemapb.FieldData)
	if !ok {
		return nil, merr.WrapErrFunctionFailedMsg("MinHash embedding failed: MinHash functionRunner return unknown data")
	}

	vectorField := fieldData.GetVectors()
	if vectorField == nil {
		return nil, merr.WrapErrFunctionFailedMsg("MinHash embedding failed: output is not a vector field")
	}

	binaryVector := vectorField.GetBinaryVector()
	if binaryVector == nil {
		return nil, merr.WrapErrFunctionFailedMsg("MinHash embedding failed: output is not a binary vector")
	}

	return funcutil.FieldDataToPlaceholderGroupBytes(fieldData)
}
