package indexparamcheck

import (
	"strconv"
	"testing"
)

func Test_CheckIntByRange(t *testing.T) {
	params := map[string]string{
		"1":  strconv.Itoa(1),
		"2":  strconv.Itoa(2),
		"3":  strconv.Itoa(3),
		"s1": "s1",
		"s2": "s2",
		"s3": "s3",
	}

	cases := []struct {
		params map[string]string
		key    string
		min    int
		max    int
		want   bool
	}{
		{params, "1", 0, 4, true},
		{params, "2", 0, 4, true},
		{params, "3", 0, 4, true},
		{params, "1", 4, 5, false},
		{params, "2", 4, 5, false},
		{params, "3", 4, 5, false},
		{params, "4", 0, 4, false},
		{params, "5", 0, 4, false},
		{params, "6", 0, 4, false},
		{params, "s1", 0, 4, false},
		{params, "s2", 0, 4, false},
		{params, "s3", 0, 4, false},
		{params, "s4", 0, 4, false},
		{params, "s5", 0, 4, false},
		{params, "s6", 0, 4, false},
	}

	for _, test := range cases {
		if got := CheckIntByRange(test.params, test.key, test.min, test.max); got != test.want {
			t.Errorf("CheckIntByRange(%v, %v, %v, %v) = %v", test.params, test.key, test.min, test.max, test.want)
		}
	}
}

func Test_CheckStrByValues(t *testing.T) {
	params := map[string]string{
		"1": strconv.Itoa(1),
		"2": strconv.Itoa(2),
		"3": strconv.Itoa(3),
	}

	cases := []struct {
		params    map[string]string
		key       string
		container []string
		want      bool
	}{
		{params, "1", []string{"1", "2", "3"}, true},
		{params, "2", []string{"1", "2", "3"}, true},
		{params, "3", []string{"1", "2", "3"}, true},
		{params, "1", []string{"4", "5", "6"}, false},
		{params, "2", []string{"4", "5", "6"}, false},
		{params, "3", []string{"4", "5", "6"}, false},
		{params, "1", []string{}, false},
		{params, "2", []string{}, false},
		{params, "3", []string{}, false},
		{params, "4", []string{"1", "2", "3"}, false},
		{params, "5", []string{"1", "2", "3"}, false},
		{params, "6", []string{"1", "2", "3"}, false},
		{params, "4", []string{"4", "5", "6"}, false},
		{params, "5", []string{"4", "5", "6"}, false},
		{params, "6", []string{"4", "5", "6"}, false},
		{params, "4", []string{}, false},
		{params, "5", []string{}, false},
		{params, "6", []string{}, false},
	}

	for _, test := range cases {
		if got := CheckStrByValues(test.params, test.key, test.container); got != test.want {
			t.Errorf("CheckStrByValues(%v, %v, %v) = %v", test.params, test.key, test.container, test.want)
		}
	}
}
