package schemautil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ValidateAddFunctionRequiresStorageV3 rejects adding a function to an existing collection
// unless the compaction infrastructure that backfills its output field into pre-existing
// sealed segments is enabled. The new function-output field is materialized only by
// compaction (bump_schema_version compaction requires a StorageV3 segment):
//   - common.storage.useLoonFFI: StorageV3 mode (bump works only on V3 segments; TEXT needs V3).
//   - dataCoord.compaction.bumpSchemaVersion.enabled: the bumpSchemaVersionPolicy that directly
//     backfills pre-existing V3 segments; it defaults to false, and there is no direct trigger
//     on the add-function DDL, so without it an already-V3 old segment never receives the output.
//   - dataCoord.compaction.storageVersion.enabled: the storageVersionUpgradePolicy that first
//     mix-upgrades a pre-existing V2 segment to V3 so it can be bumped.
//
// Without all three, the DDL would succeed while old rows silently never get the function output.
// New writes always compute the function output at flush, so create_collection with a function is
// unaffected; this guard applies only to add-function on an existing collection. See issue #51167.
func ValidateAddFunctionRequiresStorageV3() error {
	if !paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
		return merr.WrapErrServiceUnavailableMsg("adding a function field requires StorageV3; enable common.storage.useLoonFFI")
	}
	if !paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.GetAsBool() {
		return merr.WrapErrServiceUnavailableMsg("adding a function field requires schema-bump compaction to backfill existing segments; enable dataCoord.compaction.bumpSchemaVersion.enabled")
	}
	if !paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.GetAsBool() {
		return merr.WrapErrServiceUnavailableMsg("adding a function field requires the storage-version upgrade compaction; enable dataCoord.compaction.storageVersion.enabled")
	}
	return nil
}

// ValidateAddFunctionInputNotText rejects adding a BM25/MinHash function whose input is a
// TEXT field. TEXT columns are stored as binary LOB references, so when the async backfill
// compaction materializes the function output for pre-existing segments it reads the input
// back as *array.Binary and hard-fails in stringInputsFromRecord ("cannot materialize bm25
// from text binary values without lob decoding"); the add-function DDL would return success
// while the backfill silently fails and old rows never get the output. Reject it up front
// (fail-fast). VarChar inputs stay allowed, and create_collection with such a function is
// unaffected -- its output is computed at flush from the raw text. Distinct from the
// storage-version gate (ValidateAddFunctionRequiresStorageV3): this is a request-content
// input-type constraint, not an environment/config check. See issue #51167.
func ValidateAddFunctionInputNotText(schema *schemapb.CollectionSchema, function *schemapb.FunctionSchema) error {
	switch function.GetType() {
	case schemapb.FunctionType_BM25, schemapb.FunctionType_MinHash:
	default:
		// Only BM25/MinHash are materialized from string input during backfill.
		return nil
	}
	fieldByName := make(map[string]*schemapb.FieldSchema, len(schema.GetFields()))
	for _, f := range schema.GetFields() {
		fieldByName[f.GetName()] = f
	}
	for _, name := range function.GetInputFieldNames() {
		if f, ok := fieldByName[name]; ok && f.GetDataType() == schemapb.DataType_Text {
			return merr.WrapErrParameterInvalidMsg(
				"adding a %s function with a TEXT input field (%s) is not supported: its output cannot be backfilled into existing segments; use a VARCHAR input field",
				function.GetType().String(), name)
		}
	}
	return nil
}
