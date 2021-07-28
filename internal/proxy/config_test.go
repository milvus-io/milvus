package proxy

import (
	"testing"

	"github.com/milvus-io/milvus/internal/util/typedef"
)

func TestInScorePositivelyRelatedToDistanceMetrics(t *testing.T) {
	cases := []struct {
		item typedef.MetricType
		want bool
	}{
		{typedef.MetricIP, true},
		{typedef.MetricJaccard, true},
		{typedef.MetricTanimoto, true},
		{typedef.MetricL2, false},
		{typedef.MetricHamming, false},
		{typedef.MetricSuperStructure, false},
		{typedef.MetricSubStructure, false},
	}

	for _, test := range cases {
		if got := inScorePositivelyRelatedToDistanceMetrics(test.item); got != test.want {
			t.Errorf("inScorePositivelyRelatedToDistanceMetrics(%v) = %v", test.item, test.want)
		}
	}
}

func TestInScoreNegativelyRelatedToDistanceMetrics(t *testing.T) {
	cases := []struct {
		item typedef.MetricType
		want bool
	}{
		{typedef.MetricIP, false},
		{typedef.MetricJaccard, false},
		{typedef.MetricTanimoto, false},
		{typedef.MetricL2, true},
		{typedef.MetricHamming, true},
		{typedef.MetricSuperStructure, true},
		{typedef.MetricSubStructure, true},
	}

	for _, test := range cases {
		if got := inScoreNegativelyRelatedToDistanceMetrics(test.item); got != test.want {
			t.Errorf("inScoreNegativelyRelatedToDistanceMetrics(%v) = %v", test.item, test.want)
		}
	}
}
