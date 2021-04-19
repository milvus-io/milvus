// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxynode

import (
	"strconv"
	"testing"
)

// TODO: add more test cases which `ConfAdapter.CheckTrain` return false,
//       for example, maybe we can copy test cases from regression test later.

func TestBaseConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newBaseConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("BaseConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestIVFConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(1024),
		Metric: L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newIVFConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("IVFConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestIVFPQConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(1024),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newIVFPQConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("IVFPQConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestIVFSQConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		NBITS:  strconv.Itoa(8),
		Metric: L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newIVFSQConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("IVFSQConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestBinIDMAPConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: JACCARD,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newBinIDMAPConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("BinIDMAPConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestBinIVFConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: JACCARD,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newBinIVFConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("BinIVFConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestNSGConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:          strconv.Itoa(128),
		NLIST:        strconv.Itoa(163),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newNSGConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("NSGConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestHNSWConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newHNSWConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("HNSWConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestANNOYConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NTREES: strconv.Itoa(4),
		Metric: L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newANNOYConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("ANNOYConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestRHNSWFlatConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newRHNSWFlatConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("RHNSWFlatConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestRHNSWPQConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		PQM:            strconv.Itoa(8),
		EFConstruction: strconv.Itoa(200),
		Metric:         L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newRHNSWPQConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("RHNSWPQConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestRHNSWSQConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newRHNSWSQConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("RHNSWSQConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestNGTPANNGConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newNGTPANNGConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("NGTPANNGConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}

func TestNGTONNGConfAdapter_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           L2,
	}
	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
	}

	adapter := newNGTONNGConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("NGTONNGConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}
