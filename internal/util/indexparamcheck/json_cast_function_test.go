package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestJSONCastFunctionCompatibility(t *testing.T) {
	valid, unknown, empty := "STRING_TO_DOUBLE", "STRING_TO_INT64", ""
	for _, name := range []string{"INVERTED", "HYBRID", "BITMAP", "STL_SORT"} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				cast     string
				function *string
				valid    bool
			}{
				{name: "INT64 without function", cast: "INT64", valid: true},
				{name: "INT64 with double function", cast: "INT64", function: &valid},
				{name: "INT64 with unknown function", cast: "INT64", function: &unknown},
				{name: "INT64 with empty function", cast: "INT64", function: &empty},
				{name: "VARCHAR without function", cast: "VARCHAR", valid: true},
				{name: "VARCHAR with double function", cast: "VARCHAR", function: &valid},
				{name: "DOUBLE with double function", cast: "DOUBLE", function: &valid, valid: name != "BITMAP"},
				{name: "DOUBLE with unknown function", cast: "DOUBLE", function: &unknown},
				{name: "DOUBLE with empty function", cast: "DOUBLE", function: &empty},
				{name: "ARRAY_DOUBLE with scalar function", cast: "ARRAY_DOUBLE", function: &valid},
			} {
				t.Run(tc.name, func(t *testing.T) {
					params := map[string]string{common.IndexTypeKey: name, common.JSONCastTypeKey: tc.cast, common.JSONPathKey: "json['a']"}
					if tc.function != nil {
						params[common.JSONCastFunctionKey] = *tc.function
					}
					err := ValidateFieldIndexParams(&schemapb.FieldSchema{FieldID: 100, Name: "json", DataType: schemapb.DataType_JSON}, params)
					if tc.valid {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, merr.ErrParameterInvalid)
					}
				})
			}
		})
	}
}
