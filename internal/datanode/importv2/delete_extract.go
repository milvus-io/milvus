package importv2

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ExtractDeleteData reads the primary key column of data (keyed by pkField's
// field ID) and turns each value into a delete record stamped with ts.
//
// ts must be the import job's request timetick, not the commit timestamp of
// the rows this job writes. segcore skips a delete when delete_ts <= insert_ts,
// so using the request timetick lets a delete remove pre-existing rows while
// leaving rows written later by the same job (stamped at commit time) intact.
func ExtractDeleteData(data *storage.InsertData, pkField *schemapb.FieldSchema, ts uint64) (*storage.DeleteData, error) {
	fieldData, ok := data.Data[pkField.GetFieldID()]
	if !ok {
		return nil, merr.WrapErrImportFailedMsg("primary key field %s not found in delete-key file", pkField.GetName())
	}

	rowNum := fieldData.RowNum()
	pks := make([]storage.PrimaryKey, 0, rowNum)
	tss := make([]storage.Timestamp, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		pk, err := storage.GenPrimaryKeyByRawData(fieldData.GetRow(i), pkField.GetDataType())
		if err != nil {
			return nil, err
		}
		pks = append(pks, pk)
		tss = append(tss, ts)
	}
	return storage.NewDeleteData(pks, tss), nil
}
