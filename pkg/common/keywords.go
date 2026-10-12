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
// used as a field name. Core operators (like, and, or, not, in) and null are
// reserved in every casing. Contextual keywords such as iso and interval stay
// usable as field names; other reserved words retain the exact casings listed
// in FieldNameKeywords.
func IsFieldNameKeyword(fieldName string) bool {
	if _, ok := FieldNameKeywords[fieldName]; ok {
		return true
	}
	switch strings.ToLower(fieldName) {
	case "like", "and", "or", "not", "in", "null":
		return true
	default:
		return false
	}
}
