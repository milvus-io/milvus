package proxynode

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jarcoal/httpmock"
)

func TestGetPulsarConfig(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	runtimeConfig := make(map[string]interface{})
	runtimeConfig[PulsarMaxMessageSizeKey] = strconv.FormatInt(5*1024*1024, 10)

	protocol := "http"
	ip := "pulsar"
	port := "18080"
	url := "/admin/v2/brokers/configuration/runtime"
	httpmock.RegisterResponder("GET", protocol+"://"+ip+":"+port+url,
		func(req *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, runtimeConfig)
		},
	)

	ret, err := GetPulsarConfig(protocol, ip, port, url)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(ret), len(runtimeConfig))
	assert.Equal(t, len(ret), 1)
	for key, value := range ret {
		assert.Equal(t, fmt.Sprintf("%v", value), fmt.Sprintf("%v", runtimeConfig[key]))
	}
}

func TestSliceContain(t *testing.T) {
	strSlice := []string{"test", "for", "SliceContain"}
	intSlice := []int{1, 2, 3}

	cases := []struct {
		s    interface{}
		item interface{}
		want bool
	}{
		{strSlice, "test", true},
		{strSlice, "for", true},
		{strSlice, "SliceContain", true},
		{strSlice, "tests", false},
		{intSlice, 1, true},
		{intSlice, 2, true},
		{intSlice, 3, true},
		{intSlice, 4, false},
	}

	for _, test := range cases {
		if got := SliceContain(test.s, test.item); got != test.want {
			t.Errorf("SliceContain(%v, %v) = %v", test.s, test.item, test.want)
		}
	}
}

func TestSliceSetEqual(t *testing.T) {
	cases := []struct {
		s1   interface{}
		s2   interface{}
		want bool
	}{
		{[]int{}, []int{}, true},
		{[]string{}, []string{}, true},
		{[]int{1, 2, 3}, []int{3, 2, 1}, true},
		{[]int{1, 2, 3}, []int{1, 2, 3}, true},
		{[]int{1, 2, 3}, []int{}, false},
		{[]int{1, 2, 3}, []int{1, 2}, false},
		{[]int{1, 2, 3}, []int{4, 5, 6}, false},
		{[]string{"test", "for", "SliceSetEqual"}, []string{"SliceSetEqual", "test", "for"}, true},
		{[]string{"test", "for", "SliceSetEqual"}, []string{"test", "for", "SliceSetEqual"}, true},
		{[]string{"test", "for", "SliceSetEqual"}, []string{"test", "for"}, false},
		{[]string{"test", "for", "SliceSetEqual"}, []string{}, false},
		{[]string{"test", "for", "SliceSetEqual"}, []string{"test", "for", "SliceContain"}, false},
	}

	for _, test := range cases {
		if got := SliceSetEqual(test.s1, test.s2); got != test.want {
			t.Errorf("SliceSetEqual(%v, %v) = %v", test.s1, test.s2, test.want)
		}
	}
}

func TestSortedSliceEqual(t *testing.T) {
	sortSlice := func(slice interface{}, less func(i, j int) bool) {
		sort.Slice(slice, less)
	}
	intSliceAfterSort := func(slice []int) []int {
		sortSlice(slice, func(i, j int) bool {
			return slice[i] <= slice[j]
		})
		return slice
	}
	stringSliceAfterSort := func(slice []string) []string {
		sortSlice(slice, func(i, j int) bool {
			return slice[i] <= slice[j]
		})
		return slice
	}

	cases := []struct {
		s1   interface{}
		s2   interface{}
		want bool
	}{
		{intSliceAfterSort([]int{}), intSliceAfterSort([]int{}), true},
		{stringSliceAfterSort([]string{}), stringSliceAfterSort([]string{}), true},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{3, 2, 1}), true},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{1, 2, 3}), true},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{}), false},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{1, 2}), false},
		{intSliceAfterSort([]int{1, 2, 3}), intSliceAfterSort([]int{4, 5, 6}), false},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{"SliceSetEqual", "test", "for"}), true},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), true},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{"test", "for"}), false},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{}), false},
		{stringSliceAfterSort([]string{"test", "for", "SliceSetEqual"}), stringSliceAfterSort([]string{"test", "for", "SliceContain"}), false},
	}

	for _, test := range cases {
		if got := SortedSliceEqual(test.s1, test.s2); got != test.want {
			t.Errorf("SliceSetEqual(%v, %v) = %v", test.s1, test.s2, test.want)
		}
	}
}
