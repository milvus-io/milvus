package common

import "strings"

var FieldNameKeywords = map[string]struct{}{
	"$meta":              {},
	"like":               {},
	"exists":             {},
	"EXISTS":             {},
	"and":                {},
	"or":                 {},
	"not":                {},
	"in":                 {},
	"json_contains":      {},
	"JSON_CONTAINS":      {},
	"json_contains_all":  {},
	"JSON_CONTAINS_ALL":  {},
	"json_contains_any":  {},
	"JSON_CONTAINS_ANY":  {},
	"array_contains":     {},
	"ARRAY_CONTAINS":     {},
	"array_contains_all": {},
	"ARRAY_CONTAINS_ALL": {},
	"array_contains_any": {},
	"ARRAY_CONTAINS_ANY": {},
	"array_length":       {},
	"ARRAY_LENGTH":       {},
	"true":               {},
	"True":               {},
	"TRUE":               {},
	"false":              {},
	"False":              {},
	"FALSE":              {},
	"text_match":         {},
	"TEXT_MATCH":         {},
	"phrase_match":       {},
	"PHRASE_MATCH":       {},
	"random_sample":      {},
	"RANDOM_SAMPLE":      {},
}

// IsFieldNameKeyword reports whether fieldName is a reserved word that cannot be
// used as a field name. `null` is matched case-insensitively — any casing of NULL
// is rejected — to stay consistent with the expression parser, which rejects a
// bare NULL literal regardless of casing (issue #50882). It is therefore handled
// here rather than enumerated in FieldNameKeywords; the other keywords match the
// exact casings listed there.
func IsFieldNameKeyword(fieldName string) bool {
	if _, ok := FieldNameKeywords[fieldName]; ok {
		return true
	}
	return strings.EqualFold(fieldName, "null")
}
