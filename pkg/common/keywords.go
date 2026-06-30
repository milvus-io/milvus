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
	"null":               {},
	"Null":               {},
	"NULL":               {},
	"text_match":         {},
	"TEXT_MATCH":         {},
	"phrase_match":       {},
	"PHRASE_MATCH":       {},
	"random_sample":      {},
	"RANDOM_SAMPLE":      {},
}

func IsFieldNameKeyword(fieldName string) bool {
	if _, ok := FieldNameKeywords[fieldName]; ok {
		return true
	}
	return strings.EqualFold(fieldName, "null")
}
