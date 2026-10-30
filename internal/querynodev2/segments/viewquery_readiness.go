package segments

import (
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ValidateQueryViewFields checks the capabilities that schema-only native
// reopen cannot provide. Missing ordinary nullable/default columns can be
// projected, but missing Function outputs cannot be treated as empty vectors.
// Call after loading/reopening, before acknowledging the requesting view.
func (s *LocalSegment) ValidateQueryViewFields(collection *Collection) error {
	for _, field := range typeutil.GetAllFieldSchemas(collection.Schema()) {
		id := field.GetFieldID()
		if !field.GetIsFunctionOutput() || !collection.loadFields.Contain(id) {
			continue
		}
		if !s.HasFieldData(id) && !s.ExistIndex(id) {
			return merr.WrapErrServiceNotReadyMsg("segment %d has not materialized function output field %d for query view schema %d", s.ID(), id, collection.Schema().GetVersion())
		}
	}
	return nil
}
