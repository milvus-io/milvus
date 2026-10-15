package viewquery

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReferencedSchemaFields includes every physical dependency of the serialized
// plan and request, including nested expressions, aggregate/order/group fields
// and function inputs. Reflect over messages so a new Expr node containing a
// ColumnInfo does not silently bypass removal validation.
func ReferencedSchemaFields(request proto.Message, schema *schemapb.CollectionSchema, requiredFields ...int64) ([]int64, error) {
	fields := make(map[int64]struct{})
	for _, field := range requiredFields {
		fields[field] = struct{}{}
	}
	if err := collectSchemaFields(request.ProtoReflect(), fields, schema); err != nil {
		return nil, err
	}
	// Every query can need PKs for reduction/requery, even when not projected.
	if schema != nil {
		for _, field := range typeutil.GetAllFieldSchemas(schema) {
			if field.GetIsPrimaryKey() {
				fields[field.GetFieldID()] = struct{}{}
			}
		}
	}
	// TTL is an implicit filter supplied by the selected read schema.
	for _, property := range schema.GetProperties() {
		if property.GetKey() == common.CollectionTTLFieldKey {
			for _, field := range typeutil.GetAllFieldSchemas(schema) {
				if field.GetName() == property.GetValue() {
					fields[field.GetFieldID()] = struct{}{}
				}
			}
		}
	}
	// Expand transitive function and StructArray dependencies to a fixed point.
	for changed := true; changed; {
		before := len(fields)
		for _, function := range schema.GetFunctions() {
			for _, output := range function.GetOutputFieldIds() {
				if _, used := fields[output]; used {
					for _, input := range function.GetInputFieldIds() {
						fields[input] = struct{}{}
					}
				}
			}
		}
		for _, structure := range schema.GetStructArrayFields() {
			if _, used := fields[structure.GetFieldID()]; used {
				for _, field := range structure.GetFields() {
					fields[field.GetFieldID()] = struct{}{}
				}
			}
		}
		changed = len(fields) != before
	}
	result := make([]int64, 0, len(fields))
	for id := range fields {
		if id >= common.StartOfUserFieldID {
			result = append(result, id)
		}
	}
	slices.Sort(result)
	return result, nil
}

func collectSchemaFields(message protoreflect.Message, fields map[int64]struct{}, schema *schemapb.CollectionSchema) error {
	if functionChain, ok := message.Interface().(*schemapb.FunctionChain); ok && schema != nil {
		repr, err := chain.ProtoChainToRepr(functionChain)
		if err != nil {
			return err
		}
		input, err := chain.CompileDataFrameInputPlan(repr, schema)
		if err != nil {
			return err
		}
		for _, id := range input.PhysicalFieldIDs() {
			fields[id] = struct{}{}
		}
	}

	var result error
	message.Range(func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		name := string(descriptor.Name())
		if name == "serialized_expr_plan" {
			plan := &planpb.PlanNode{}
			if err := proto.Unmarshal(value.Bytes(), plan); err != nil {
				result = merr.WrapErrSerializationFailed(err, "decode QueryView plan")
				return false
			}
			result = collectSchemaFields(plan.ProtoReflect(), fields, schema)
			return result == nil
		}
		// Numeric field references use these canonical protobuf names. Do not
		// interpret unrelated numeric values (PK literals, timestamps, IDs) as fields.
		fieldID := name == "field_id" || strings.HasSuffix(name, "_field_id") || strings.HasSuffix(name, "_field_ids") || name == "output_fields_id"
		visit := func(v protoreflect.Value) {
			if descriptor.Kind() == protoreflect.MessageKind {
				result = collectSchemaFields(v.Message(), fields, schema)
			} else if fieldID && descriptor.Kind() == protoreflect.Int64Kind {
				fields[v.Int()] = struct{}{}
			}
		}
		if descriptor.IsMap() {
			return true
		}
		if descriptor.IsList() {
			list := value.List()
			for i := 0; i < list.Len() && result == nil; i++ {
				visit(list.Get(i))
			}
		} else {
			visit(value)
		}
		return result == nil
	})
	return result
}

func requestSchemaVersion(request proto.Message) int32 {
	switch request := request.(type) {
	case *internalpb.SearchRequest:
		return request.GetSchemaVersion()
	case *internalpb.RetrieveRequest:
		return request.GetSchemaVersion()
	default:
		panic("unexpected QueryView request type")
	}
}

// ValidateSchemaBinding checks a Phase 1 proof before executing any task. Legacy
// plans without a binding keep their wire compatibility; new planners always
// attach one, including for schema version zero and an empty worker list.
func ValidateSchemaBinding(request proto.Message, mvcc *viewpb.QueryPlanMVCC, schema *schemapb.CollectionSchema) error {
	binding := mvcc.GetSchemaBinding()
	if binding == nil {
		return nil
	}
	if binding.GetSchemaVersion() != requestSchemaVersion(request) ||
		binding.GetTransformingTimetick() != mvcc.GetTransformingTimetick() ||
		binding.GetGrowingTimetick() != mvcc.GetGrowingTimetick() {
		return viewerror.NewViewInvalidated("query schema/MVCC differs from its validated binding")
	}
	fields, err := ReferencedSchemaFields(request, schema)
	if err != nil {
		return err
	}
	for _, field := range fields {
		if !slices.Contains(binding.GetFieldIds(), field) {
			return viewerror.NewViewInvalidated("field %d was not validated by the query plan", field)
		}
	}
	return nil
}
