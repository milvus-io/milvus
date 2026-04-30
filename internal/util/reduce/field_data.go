package reduce

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// FindFieldDataByID returns the first FieldData whose FieldId matches fieldID,
// or nil if none. Callers rely on FieldId being set upstream; when reducers
// re-emit group-by columns they must preserve FieldId for downstream lookup.
func FindFieldDataByID(fieldsData []*schemapb.FieldData, fieldID int64) *schemapb.FieldData {
	for _, fd := range fieldsData {
		if fd.GetFieldId() == fieldID {
			return fd
		}
	}
	return nil
}

func FindGroupByFieldData(data *schemapb.SearchResultData, fieldID int64, allowSingularFallback bool) *schemapb.FieldData {
	if data == nil {
		return nil
	}
	if fd := FindFieldDataByID(data.GetGroupByFieldValues(), fieldID); fd != nil {
		return fd
	}
	if allowSingularFallback {
		return data.GetGroupByFieldValue()
	}
	return nil
}

func ValidateGroupByFieldsPresent(searchResultData []*schemapb.SearchResultData, fieldIDs []int64, allowSingularFallback bool) error {
	for resultIdx, data := range searchResultData {
		if data == nil || typeutil.GetSizeOfIDs(data.GetIds()) == 0 {
			continue
		}
		for _, fieldID := range fieldIDs {
			if FindGroupByFieldData(data, fieldID, allowSingularFallback) == nil {
				return fmt.Errorf("group-by field %d missing from search result %d", fieldID, resultIdx)
			}
		}
	}
	return nil
}

// WriteGroupByFieldValues emits one FieldData per composite-key field into
// ret.GroupByFieldValues, pulling values for each accepted row from the
// source shard it originated in. Each emitted FieldData carries FieldId so
// downstream consumers can look up by id rather than position.
//
// Shared by delegator-side SearchGroupByReduce and proxy-side cross-shard
// reduce, so both layers produce a byte-for-byte identical field-17 payload.
func WriteGroupByFieldValues(
	ret *schemapb.SearchResultData,
	acceptedRows []RowRef,
	sources []*schemapb.SearchResultData,
	fieldIDs []int64,
) error {
	if len(fieldIDs) == 0 || len(acceptedRows) == 0 {
		return nil
	}
	ret.GroupByFieldValues = make([]*schemapb.FieldData, 0, len(fieldIDs))
	for _, fid := range fieldIDs {
		iters := make([]func(int) any, len(sources))
		var template *schemapb.FieldData
		for i, srd := range sources {
			fd := FindFieldDataByID(srd.GetGroupByFieldValues(), fid)
			// N=1 legacy path: upstream wrote the group-by column to the
			// singular channel (SearchResultData.group_by_field_value) without
			// a FieldId stamp. Fall back to that channel so the unified
			// proxy-side reducer can read legacy inputs and still emit the
			// plural output downstream consumers expect.
			if fd == nil && len(fieldIDs) == 1 {
				fd = srd.GetGroupByFieldValue()
			}
			if fd == nil {
				continue
			}
			iters[i] = typeutil.GetDataIterator(fd)
			if template == nil {
				template = fd
			}
		}
		if template == nil {
			return fmt.Errorf("group-by field %d missing from all source shards", fid)
		}

		builder, err := typeutil.NewFieldDataBuilder(template.GetType(), true, len(acceptedRows))
		if err != nil {
			return err
		}
		for _, row := range acceptedRows {
			iter := iters[row.ResultIdx]
			if iter == nil {
				return fmt.Errorf("group-by field %d missing at source shard index %d", fid, row.ResultIdx)
			}
			builder.Add(iter(int(row.RowIdx)))
		}
		fd := builder.Build()
		fd.FieldId = fid
		fd.FieldName = template.GetFieldName()
		ret.GroupByFieldValues = append(ret.GroupByFieldValues, fd)
	}
	return nil
}
