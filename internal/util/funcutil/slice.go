package funcutil

import "reflect"

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

func SliceSetEqual(s1 interface{}, s2 interface{}) bool {
	ss1 := reflect.ValueOf(s1)
	ss2 := reflect.ValueOf(s2)
	if ss1.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss2.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss1.Len() != ss2.Len() {
		return false
	}
	for i := 0; i < ss1.Len(); i++ {
		if !SliceContain(s2, ss1.Index(i).Interface()) {
			return false
		}
	}
	return true
}

func SortedSliceEqual(s1 interface{}, s2 interface{}) bool {
	ss1 := reflect.ValueOf(s1)
	ss2 := reflect.ValueOf(s2)
	if ss1.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss2.Kind() != reflect.Slice {
		panic("expect a slice")
	}
	if ss1.Len() != ss2.Len() {
		return false
	}
	for i := 0; i < ss1.Len(); i++ {
		if ss2.Index(i).Interface() != ss1.Index(i).Interface() {
			return false
		}
	}
	return true
}
