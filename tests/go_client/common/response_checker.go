package common

import (
	"fmt"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/stretchr/testify/require"

	clientv2 "github.com/milvus-io/milvus/client/v2"
)

func CheckErr(t *testing.T, actualErr error, expErrNil bool, expErrorMsg ...string) {
	if expErrNil {
		require.NoError(t, actualErr)
	} else {
		require.Error(t, actualErr)
		switch len(expErrorMsg) {
		case 0:
			log.Fatal("expect error message should not be empty")
		case 1:
			require.ErrorContains(t, actualErr, expErrorMsg[0])
		default:
			contains := false
			for i := 0; i < len(expErrorMsg); i++ {
				if strings.Contains(actualErr.Error(), expErrorMsg[i]) {
					contains = true
				}
			}
			if !contains {
				t.FailNow()
			}
		}
	}
}

// EqualColumn assert field data is equal of two columns
func EqualColumn(t *testing.T, columnA column.Column, columnB column.Column) {
	require.Equal(t, columnA.Name(), columnB.Name())
	require.Equal(t, columnA.Type(), columnB.Type())
	switch columnA.Type() {
	case entity.FieldTypeBool:
		require.ElementsMatch(t, columnA.(*column.ColumnBool).Data(), columnB.(*column.ColumnBool).Data())
	case entity.FieldTypeInt8:
		require.ElementsMatch(t, columnA.(*column.ColumnInt8).Data(), columnB.(*column.ColumnInt8).Data())
	case entity.FieldTypeInt16:
		require.ElementsMatch(t, columnA.(*column.ColumnInt16).Data(), columnB.(*column.ColumnInt16).Data())
	case entity.FieldTypeInt32:
		require.ElementsMatch(t, columnA.(*column.ColumnInt32).Data(), columnB.(*column.ColumnInt32).Data())
	case entity.FieldTypeInt64:
		require.ElementsMatch(t, columnA.(*column.ColumnInt64).Data(), columnB.(*column.ColumnInt64).Data())
	case entity.FieldTypeFloat:
		require.ElementsMatch(t, columnA.(*column.ColumnFloat).Data(), columnB.(*column.ColumnFloat).Data())
	case entity.FieldTypeDouble:
		require.ElementsMatch(t, columnA.(*column.ColumnDouble).Data(), columnB.(*column.ColumnDouble).Data())
	case entity.FieldTypeVarChar:
		require.ElementsMatch(t, columnA.(*column.ColumnVarChar).Data(), columnB.(*column.ColumnVarChar).Data())
	case entity.FieldTypeJSON:
		log.Debug("columnA", zap.Any("data", columnA.(*column.ColumnJSONBytes).Data()))
		log.Debug("columnB", zap.Any("data", columnB.(*column.ColumnJSONBytes).Data()))
		require.ElementsMatch(t, columnA.(*column.ColumnJSONBytes).Data(), columnB.(*column.ColumnJSONBytes).Data())
	case entity.FieldTypeFloatVector:
		require.ElementsMatch(t, columnA.(*column.ColumnFloatVector).Data(), columnB.(*column.ColumnFloatVector).Data())
	case entity.FieldTypeBinaryVector:
		require.ElementsMatch(t, columnA.(*column.ColumnBinaryVector).Data(), columnB.(*column.ColumnBinaryVector).Data())
	case entity.FieldTypeArray:
		log.Info("TODO support column element type")
	default:
		log.Info("Support column type is:", zap.Any("FieldType", []entity.FieldType{entity.FieldTypeBool,
			entity.FieldTypeInt8, entity.FieldTypeInt16, entity.FieldTypeInt32, entity.FieldTypeInt64,
			entity.FieldTypeFloat, entity.FieldTypeDouble, entity.FieldTypeString, entity.FieldTypeVarChar,
			entity.FieldTypeArray, entity.FieldTypeFloatVector, entity.FieldTypeBinaryVector}))
	}
}

// CheckOutputFields check query output fields
func CheckOutputFields(t *testing.T, expFields []string, actualColumns []column.Column) {
	actualFields := make([]string, 0)
	for _, actualColumn := range actualColumns {
		actualFields = append(actualFields, actualColumn.Name())
	}
	require.ElementsMatchf(t, expFields, actualFields, fmt.Sprintf("Expected search output fields: %v, actual: %v", expFields, actualFields))
}

// CheckSearchResult check search result, check nq, topk, ids, score
func CheckSearchResult(t *testing.T, actualSearchResults []clientv2.ResultSet, expNq int, expTopK int) {
	require.Equal(t, len(actualSearchResults), expNq)
	require.Len(t, actualSearchResults, expNq)
	for _, actualSearchResult := range actualSearchResults {
		require.Equal(t, actualSearchResult.ResultCount, expTopK)
	}
}

// CheckInsertResult check insert result, ids len (insert count), ids data (pks, but no auto ids)
func CheckInsertResult(t *testing.T, expIds column.Column, insertRes clientv2.InsertResult) {
	require.Equal(t, expIds.Len(), insertRes.IDs.Len())
	require.Equal(t, expIds.Len(), int(insertRes.InsertCount))
	actualIds := insertRes.IDs
	switch expIds.Type() {
	// pk field support int64 and varchar type
	case entity.FieldTypeInt64:
		require.ElementsMatch(t, actualIds.(*column.ColumnInt64).Data(), expIds.(*column.ColumnInt64).Data())
	case entity.FieldTypeVarChar:
		require.ElementsMatch(t, actualIds.(*column.ColumnVarChar).Data(), expIds.(*column.ColumnVarChar).Data())
	default:
		log.Info("The primary field only support ", zap.Any("type", []entity.FieldType{entity.FieldTypeInt64, entity.FieldTypeVarChar}))
	}
}
