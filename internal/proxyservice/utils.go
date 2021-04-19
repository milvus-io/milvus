package proxyservice

import (
	"reflect"
)

// what if golang support generic programming
func SliceContain(s interface{}, item interface{}) bool {
	ss := reflect.ValueOf(s)
	if ss.Kind() != reflect.Slice {
		panic("SliceContain expect a slice")
	}

	for i := 0; i < ss.Len(); i++ {
		if ss.Index(i).Interface() == item {
			return true
		}
	}

	return false
}
