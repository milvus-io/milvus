package proxy

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// JoinKey is the search_params key for specifying JOIN clause
const JoinKey = "join"

// JoinSpec represents a parsed JOIN clause
type JoinSpec struct {
	TargetDBName     string   // optional database name (defaults to current db)
	TargetCollection string   // collection to join with
	JoinKey          string   // key field in the search collection
	TargetKey        string   // key field in the target collection
	WhereCondition   string   // optional filter condition on target collection
	OutputFields     []string // fields to return from target (populated from output_fields with qualified names)
	OriginalJoin     string   // original join string for logging/debugging
}

// Regex patterns for parsing JOIN clauses
var (
	// Full JOIN pattern: JOIN [db.]collection ON local_key = [db.]collection.target_key [WHERE condition]
	joinPattern = regexp.MustCompile(
		`(?i)^\s*` + // case insensitive, optional leading whitespace
			`JOIN\s+` + // JOIN keyword
			`([\w.]+)\s+` + // collection name (may include db.collection)
			`ON\s+` + // ON keyword
			`(\w+)\s*=\s*` + // local key field
			`([\w.]+)\.(\w+)` + // target_collection.target_key
			`(?:\s+WHERE\s+(.+?))?` + // optional WHERE clause (non-greedy)
			`\s*$`) // optional trailing whitespace

	// Pattern to check if input starts with JOIN keyword
	startsWithJoinPattern = regexp.MustCompile(`(?i)^\s*JOIN\s+`)

	// Pattern to detect JOIN <collection> without ON
	joinWithoutOnPattern = regexp.MustCompile(`(?i)^\s*JOIN\s+([\w.]+)\s*$`)

	// Pattern to detect missing ON keyword
	missingOnPattern = regexp.MustCompile(`(?i)^\s*JOIN\s+([\w.]+)\s+(\w+)\s*=`)

	// Pattern to detect ON without proper key = collection.key format
	onWithoutProperKeysPattern = regexp.MustCompile(`(?i)^\s*JOIN\s+([\w.]+)\s+ON\s+(\w+)?\s*$`)

	// Pattern to detect missing equals sign
	missingEqualsPattern = regexp.MustCompile(`(?i)^\s*JOIN\s+([\w.]+)\s+ON\s+(\w+)\s+[\w.]+`)

	// Pattern to detect missing target key (collection. without key)
	missingTargetKeyPattern = regexp.MustCompile(`(?i)ON\s+\w+\s*=\s*([\w.]+)\.\s*(?:WHERE|$)`)

	// Pattern to detect missing collection prefix in ON clause (key = key instead of key = collection.key)
	missingCollectionPrefixPattern = regexp.MustCompile(`(?i)ON\s+(\w+)\s*=\s*(\w+)\s*(?:WHERE|$)`)

	// Pattern to detect WHERE without condition
	whereWithoutConditionPattern = regexp.MustCompile(`(?i)WHERE\s*$`)

	// Pattern to detect double equals (== instead of =) in ON clause
	doubleEqualsInOnPattern = regexp.MustCompile(`(?i)ON\s+\w+\s*==`)
)

// ParseJoinSpec parses a single JOIN clause string into a JoinSpec
func ParseJoinSpec(joinStr string) (*JoinSpec, error) {
	trimmed := strings.TrimSpace(joinStr)
	if trimmed == "" {
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause cannot be empty. " +
				"Expected format: JOIN <collection> ON <key> = <collection>.<key> [WHERE <condition>]")
	}

	// Check if input starts with JOIN keyword
	if !startsWithJoinPattern.MatchString(trimmed) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause must start with JOIN keyword. Got: %q. "+
				"Expected format: JOIN <collection> ON <key> = <collection>.<key> [WHERE <condition>]",
			truncateForError(trimmed, 50))
	}

	// Check for JOIN <collection> without ON
	if joinWithoutOnPattern.MatchString(trimmed) {
		matches := joinWithoutOnPattern.FindStringSubmatch(trimmed)
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause is missing ON keyword. Got: 'JOIN %s'. "+
				"Expected format: JOIN %s ON <key> = %s.<key> [WHERE <condition>]",
			matches[1], matches[1], matches[1])
	}

	// Check for missing ON keyword (JOIN collection key = ...)
	if missingOnPattern.MatchString(trimmed) {
		matches := missingOnPattern.FindStringSubmatch(trimmed)
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause is missing ON keyword between collection and join condition. "+
				"Got: 'JOIN %s %s = ...'. "+
				"Expected format: JOIN %s ON %s = %s.<key>",
			matches[1], matches[2], matches[1], matches[2], matches[1])
	}

	// Check for ON without proper format
	if onWithoutProperKeysPattern.MatchString(trimmed) {
		matches := onWithoutProperKeysPattern.FindStringSubmatch(trimmed)
		collection := matches[1]
		localKey := matches[2]
		if localKey == "" {
			return nil, merr.WrapErrParameterInvalidMsg(
				"JOIN clause is missing join key after ON keyword. "+
					"Got: 'JOIN %s ON'. "+
					"Expected format: JOIN %s ON <key> = %s.<key>",
				collection, collection, collection)
		}
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause is incomplete after ON keyword. "+
				"Got: 'JOIN %s ON %s'. "+
				"Expected format: JOIN %s ON %s = %s.<key>",
			collection, localKey, collection, localKey, collection)
	}

	// Check for double equals in ON clause
	if doubleEqualsInOnPattern.MatchString(trimmed) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause has invalid operator '==' in ON condition. Use single '=' for join condition. "+
				"Got: %q. "+
				"Expected format: JOIN <collection> ON <key> = <collection>.<key>",
			truncateForError(trimmed, 80))
	}

	// Check for missing collection prefix (key = key instead of key = collection.key)
	if missingCollectionPrefixPattern.MatchString(trimmed) && !joinPattern.MatchString(trimmed) {
		matches := missingCollectionPrefixPattern.FindStringSubmatch(trimmed)
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause target key must be qualified with collection name. "+
				"Got: '%s = %s'. "+
				"Expected format: %s = <collection>.%s",
			matches[1], matches[2], matches[1], matches[2])
	}

	// Check for missing target key (collection. without key name)
	if missingTargetKeyPattern.MatchString(trimmed) {
		matches := missingTargetKeyPattern.FindStringSubmatch(trimmed)
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause is missing target key name after collection prefix. "+
				"Got: '%s.'. "+
				"Expected format: %s.<key>",
			matches[1], matches[1])
	}

	// Check for WHERE without condition
	if whereWithoutConditionPattern.MatchString(trimmed) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause has WHERE keyword but no condition. "+
				"Got: %q. "+
				"Either remove WHERE or add a condition: WHERE <condition>",
			truncateForError(trimmed, 80))
	}

	// Try to match the full pattern
	matches := joinPattern.FindStringSubmatch(trimmed)
	if matches == nil {
		// Generic error for cases not caught above
		return nil, merr.WrapErrParameterInvalidMsg(
			"invalid JOIN syntax: %q. "+
				"Expected format: JOIN <collection> ON <key> = <collection>.<key> [WHERE <condition>]. "+
				"Example: JOIN shops ON shop_id = shops.id WHERE status == 'active'",
			truncateForError(trimmed, 80))
	}

	// matches[1] = collection name (possibly db.collection)
	// matches[2] = local key field
	// matches[3] = target reference (possibly db.collection)
	// matches[4] = target key field
	// matches[5] = WHERE condition (optional)

	collectionRef := matches[1]
	localKey := matches[2]
	targetRef := matches[3]
	targetKey := matches[4]
	whereCondition := strings.TrimSpace(matches[5])

	// Parse collection reference (may be "db.collection" or just "collection")
	dbName, collectionName := parseCollectionRef(collectionRef)

	// Verify that targetRef matches collectionRef (or at least the collection part)
	targetDBName, targetCollName := parseCollectionRef(targetRef)
	if collectionName != targetCollName {
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN collection name mismatch: JOIN clause specifies '%s' but ON clause references '%s'. "+
				"The collection name must match. "+
				"Got: 'JOIN %s ON %s = %s.%s'. "+
				"Expected: 'JOIN %s ON %s = %s.%s'",
			collectionName, targetCollName,
			collectionRef, localKey, targetRef, targetKey,
			collectionRef, localKey, collectionRef, targetKey)
	}

	// If db was specified in targetRef but not in collection, use targetRef's db
	if dbName == "" && targetDBName != "" {
		dbName = targetDBName
	}

	return &JoinSpec{
		TargetDBName:     dbName,
		TargetCollection: collectionName,
		JoinKey:          localKey,
		TargetKey:        targetKey,
		WhereCondition:   whereCondition,
		OriginalJoin:     joinStr,
	}, nil
}

// ParseMultipleJoinSpecs parses multiple JOIN clauses separated by newlines
// Example:
//
//	JOIN shops ON shop_id = shops.id WHERE status == "active"
//	JOIN merchants ON merchant_id = merchants.id WHERE verified == true
func ParseMultipleJoinSpecs(joinStr string) ([]*JoinSpec, error) {
	trimmed := strings.TrimSpace(joinStr)
	if trimmed == "" {
		return nil, merr.WrapErrParameterInvalidMsg(
			"JOIN clause cannot be empty. " +
				"Expected format: JOIN <collection> ON <key> = <collection>.<key> [WHERE <condition>]")
	}

	// Split on newlines and process each line
	var joinClauses []string
	lines := strings.Split(trimmed, "\n")
	lineNum := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lineNum++

		// Check if line starts with JOIN
		if !startsWithJoinPattern.MatchString(line) {
			return nil, merr.WrapErrParameterInvalidMsg(
				"JOIN clause at line %d must start with JOIN keyword. Got: %q. "+
					"Each JOIN clause must begin with 'JOIN'. "+
					"Example:\n  JOIN shops ON shop_id = shops.id\n  JOIN merchants ON merchant_id = merchants.id",
				lineNum, truncateForError(line, 50))
		}

		joinClauses = append(joinClauses, line)
	}

	if len(joinClauses) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"no valid JOIN clauses found. Input contained only whitespace or empty lines. " +
				"Expected format: JOIN <collection> ON <key> = <collection>.<key> [WHERE <condition>]")
	}

	specs := make([]*JoinSpec, 0, len(joinClauses))
	for i, clause := range joinClauses {
		spec, err := ParseJoinSpec(clause)
		if err != nil {
			// Add context about which JOIN clause failed
			if len(joinClauses) > 1 {
				return nil, merr.WrapErrParameterInvalidMsg(
					"error in JOIN clause %d of %d: %v",
					i+1, len(joinClauses), err)
			}
			return nil, err
		}
		specs = append(specs, spec)
	}

	return specs, nil
}

// parseCollectionRef splits "db.collection" into (db, collection) or ("", collection)
func parseCollectionRef(ref string) (dbName, collectionName string) {
	parts := strings.SplitN(ref, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}

// Validate validates the JoinSpec against the search schema and verifies target collection exists
func (j *JoinSpec) Validate(ctx context.Context, searchDBName string, searchSchema *schemaInfo) error {
	// 1. Verify join key exists in search collection schema
	joinField := typeutil.GetFieldByName(searchSchema.CollectionSchema, j.JoinKey)
	if joinField == nil {
		// Suggest similar field names if available
		suggestion := suggestSimilarField(searchSchema.CollectionSchema, j.JoinKey)
		if suggestion != "" {
			return merr.WrapErrParameterInvalidMsg(
				"JOIN key field '%s' not found in search collection. Did you mean '%s'? "+
					"Available fields: %s",
				j.JoinKey, suggestion, getFieldNames(searchSchema.CollectionSchema))
		}
		return merr.WrapErrParameterInvalidMsg(
			"JOIN key field '%s' not found in search collection. "+
				"Available fields: %s",
			j.JoinKey, getFieldNames(searchSchema.CollectionSchema))
	}

	// 2. Check join key type is valid (should be a scalar type suitable for matching)
	if !isValidJoinKeyType(joinField.DataType) {
		return merr.WrapErrParameterInvalidMsg(
			"JOIN key field '%s' has type %s which is not supported for joins. "+
				"JOIN keys must be INT64 or VARCHAR type for efficient matching. "+
				"Field '%s' is of type %s.",
			j.JoinKey, joinField.DataType.String(), j.JoinKey, joinField.DataType.String())
	}

	// 3. Use default db if not specified
	targetDB := j.TargetDBName
	if targetDB == "" {
		targetDB = searchDBName
	}

	// 4. Verify target collection exists
	_, err := globalMetaCache.GetCollectionID(ctx, targetDB, j.TargetCollection)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg(
			"JOIN target collection '%s' not found in database '%s'. "+
				"Please verify the collection name is correct and the collection exists. "+
				"Original error: %v",
			j.TargetCollection, targetDB, err)
	}

	// 5. Get target collection schema and verify target key exists
	targetSchema, err := globalMetaCache.GetCollectionSchema(ctx, targetDB, j.TargetCollection)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg(
			"failed to get schema for JOIN target collection '%s' in database '%s'. "+
				"Original error: %v",
			j.TargetCollection, targetDB, err)
	}

	targetKeyField := typeutil.GetFieldByName(targetSchema.CollectionSchema, j.TargetKey)
	if targetKeyField == nil {
		suggestion := suggestSimilarField(targetSchema.CollectionSchema, j.TargetKey)
		if suggestion != "" {
			return merr.WrapErrParameterInvalidMsg(
				"JOIN target key field '%s' not found in collection '%s'. Did you mean '%s'? "+
					"Available fields: %s",
				j.TargetKey, j.TargetCollection, suggestion, getFieldNames(targetSchema.CollectionSchema))
		}
		return merr.WrapErrParameterInvalidMsg(
			"JOIN target key field '%s' not found in collection '%s'. "+
				"Available fields: %s",
			j.TargetKey, j.TargetCollection, getFieldNames(targetSchema.CollectionSchema))
	}

	// 6. Verify target key type matches join key type
	if joinField.DataType != targetKeyField.DataType {
		return merr.WrapErrParameterInvalidMsg(
			"JOIN key type mismatch: field '%s' in search collection is %s, "+
				"but field '%s' in target collection '%s' is %s. "+
				"JOIN keys must have matching types.",
			j.JoinKey, joinField.DataType.String(),
			j.TargetKey, j.TargetCollection, targetKeyField.DataType.String())
	}

	// Store resolved db name
	j.TargetDBName = targetDB

	return nil
}

// ExtractJoinOutputFields extracts qualified field names from output_fields that belong to this join
// e.g., from ["item_id", "shops.shop_name", "shops.rating"], extract ["shop_name", "rating"] for shops collection
func (j *JoinSpec) ExtractJoinOutputFields(outputFields []string) []string {
	prefix := j.TargetCollection + "."
	var fields []string

	for _, field := range outputFields {
		if strings.HasPrefix(field, prefix) {
			// Remove the collection prefix to get the actual field name
			fieldName := strings.TrimPrefix(field, prefix)
			fields = append(fields, fieldName)
		}
	}

	j.OutputFields = fields
	return fields
}

// isValidJoinKeyType checks if the data type is valid for use as a join key
func isValidJoinKeyType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Int64, schemapb.DataType_VarChar:
		return true
	default:
		return false
	}
}

// String returns a string representation of the JoinSpec for logging
func (j *JoinSpec) String() string {
	where := ""
	if j.WhereCondition != "" {
		where = fmt.Sprintf(" WHERE %s", j.WhereCondition)
	}
	db := ""
	if j.TargetDBName != "" {
		db = j.TargetDBName + "."
	}
	return fmt.Sprintf("JOIN %s%s ON %s = %s%s.%s%s",
		db, j.TargetCollection, j.JoinKey, db, j.TargetCollection, j.TargetKey, where)
}

// truncateForError truncates a string for display in error messages
func truncateForError(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// getFieldNames returns a comma-separated list of field names from a schema
func getFieldNames(schema *schemapb.CollectionSchema) string {
	var names []string
	for _, field := range schema.GetFields() {
		names = append(names, field.GetName())
	}
	if len(names) == 0 {
		return "(no fields)"
	}
	return strings.Join(names, ", ")
}

// suggestSimilarField attempts to find a field name similar to the given name
func suggestSimilarField(schema *schemapb.CollectionSchema, fieldName string) string {
	fieldNameLower := strings.ToLower(fieldName)
	for _, field := range schema.GetFields() {
		// Check for case-insensitive match
		if strings.ToLower(field.GetName()) == fieldNameLower {
			return field.GetName()
		}
		// Check for common suffixes/prefixes
		if strings.HasSuffix(strings.ToLower(field.GetName()), "_id") &&
			strings.HasSuffix(fieldNameLower, "_id") {
			if strings.TrimSuffix(strings.ToLower(field.GetName()), "_id") ==
				strings.TrimSuffix(fieldNameLower, "_id") {
				return field.GetName()
			}
		}
	}
	return ""
}
