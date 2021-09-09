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

package indexparamcheck

import (
	"strconv"
	"testing"
)

// TODO: add more test cases which `ConfAdapter.CheckTrain` return false,
//       for example, maybe we can copy test cases from regression test later.

func invalidIVFParamsMin() map[string]string {
	invalidIVFParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(MinNList - 1),
		Metric: L2,
	}
	return invalidIVFParams
}

func invalidIVFParamsMax() map[string]string {
	invalidIVFParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(MaxNList + 1),
		Metric: L2,
	}
	return invalidIVFParams
}

func copyParams(original map[string]string) map[string]string {
	result := make(map[string]string)
	for key, value := range original {
		result[key] = value
	}
	return result
}

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
		{invalidIVFParamsMin(), false},
		{invalidIVFParamsMax(), false},
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

	validParamsWithoutNbits := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(1024),
		IVFM:   strconv.Itoa(4),
		Metric: L2,
	}

	validParamsWithoutDim := map[string]string{
		NLIST:  strconv.Itoa(1024),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: L2,
	}

	invalidParamsDim := copyParams(validParams)
	invalidParamsDim[DIM] = "NAN"

	invalidParamsNbits := copyParams(validParams)
	invalidParamsNbits[NBITS] = "NAN"

	invalidParamsWithoutIVF := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(1024),
		NBITS:  strconv.Itoa(8),
		Metric: L2,
	}

	invalidParamsIVF := copyParams(validParams)
	invalidParamsIVF[IVFM] = "NAN"

	invalidParamsM := copyParams(validParams)
	invalidParamsM[IndexMode] = GPUMode
	invalidParamsM[DIM] = strconv.Itoa(65536)

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{validParamsWithoutNbits, true},
		{invalidIVFParamsMin(), false},
		{invalidIVFParamsMax(), false},
		{validParamsWithoutDim, true},
		{invalidParamsDim, false},
		{invalidParamsNbits, false},
		{invalidParamsWithoutIVF, false},
		{invalidParamsIVF, false},
		{invalidParamsM, false},
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
		{invalidIVFParamsMin(), false},
		{invalidIVFParamsMax(), false},
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

	invalidParams := copyParams(validParams)
	invalidParams[Metric] = L2

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidIVFParamsMin(), false},
		{invalidIVFParamsMax(), false},
		{invalidParams, false},
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

	invalidMatricParams := copyParams(validParams)
	invalidMatricParams[Metric] = JACCARD

	invalidKNNGParamsMin := copyParams(validParams)
	invalidKNNGParamsMin[KNNG] = strconv.Itoa(MinKNNG - 1)

	invalidKNNGParamsMax := copyParams(validParams)
	invalidKNNGParamsMax[KNNG] = strconv.Itoa(MaxKNNG + 1)

	invalidLengthParamsMin := copyParams(validParams)
	invalidLengthParamsMin[SearchLength] = strconv.Itoa(MinSearchLength - 1)

	invalidLengthParamsMax := copyParams(validParams)
	invalidLengthParamsMax[SearchLength] = strconv.Itoa(MaxSearchLength + 1)

	invalidDegreeParamsMin := copyParams(validParams)
	invalidDegreeParamsMin[OutDegree] = strconv.Itoa(MinOutDegree - 1)

	invalidDegreeParamsMax := copyParams(validParams)
	invalidDegreeParamsMax[OutDegree] = strconv.Itoa(MaxOutDegree + 1)

	invalidPoolParamsMin := copyParams(validParams)
	invalidPoolParamsMin[CANDIDATE] = strconv.Itoa(MinCandidatePoolSize - 1)

	invalidPoolParamsMax := copyParams(validParams)
	invalidPoolParamsMax[CANDIDATE] = strconv.Itoa(MaxCandidatePoolSize + 1)

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidMatricParams, false},
		{invalidKNNGParamsMin, false},
		{invalidKNNGParamsMax, false},
		{invalidLengthParamsMin, false},
		{invalidLengthParamsMax, false},
		{invalidDegreeParamsMin, false},
		{invalidDegreeParamsMax, false},
		{invalidPoolParamsMin, false},
		{invalidPoolParamsMax, false},
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

	invalidEfParamsMin := copyParams(validParams)
	invalidEfParamsMin[EFConstruction] = strconv.Itoa(HNSWMinEfConstruction - 1)

	invalidEfParamsMax := copyParams(validParams)
	invalidEfParamsMax[EFConstruction] = strconv.Itoa(HNSWMaxEfConstruction + 1)

	invalidMParamsMin := copyParams(validParams)
	invalidMParamsMin[HNSWM] = strconv.Itoa(HNSWMinM - 1)

	invalidMParamsMax := copyParams(validParams)
	invalidMParamsMax[HNSWM] = strconv.Itoa(HNSWMaxM + 1)

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidEfParamsMin, false},
		{invalidEfParamsMax, false},
		{invalidMParamsMin, false},
		{invalidMParamsMax, false},
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

	invalidTreeParamsMin := copyParams(validParams)
	invalidTreeParamsMin[NTREES] = strconv.Itoa(MinNTrees - 1)

	invalidTreeParamsMax := copyParams(validParams)
	invalidTreeParamsMax[NTREES] = strconv.Itoa(MaxNTrees + 1)

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidTreeParamsMin, false},
		{invalidTreeParamsMax, false},
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

	invalidEfParamsMin := copyParams(validParams)
	invalidEfParamsMin[EFConstruction] = strconv.Itoa(HNSWMinEfConstruction - 1)

	invalidEfParamsMax := copyParams(validParams)
	invalidEfParamsMax[EFConstruction] = strconv.Itoa(HNSWMaxEfConstruction + 1)

	invalidMParamsMin := copyParams(validParams)
	invalidMParamsMin[HNSWM] = strconv.Itoa(HNSWMinM - 1)

	invalidMParamsMax := copyParams(validParams)
	invalidMParamsMax[HNSWM] = strconv.Itoa(HNSWMaxM + 1)

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidEfParamsMin, false},
		{invalidEfParamsMax, false},
		{invalidMParamsMin, false},
		{invalidMParamsMax, false},
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

	invalidMatricParams := copyParams(validParams)
	invalidMatricParams[Metric] = JACCARD

	invalidEfParamsMin := copyParams(validParams)
	invalidEfParamsMin[EFConstruction] = strconv.Itoa(HNSWMinEfConstruction - 1)

	invalidEfParamsMax := copyParams(validParams)
	invalidEfParamsMax[EFConstruction] = strconv.Itoa(HNSWMaxEfConstruction + 1)

	invalidMParamsMin := copyParams(validParams)
	invalidMParamsMin[HNSWM] = strconv.Itoa(HNSWMinM - 1)

	invalidMParamsMax := copyParams(validParams)
	invalidMParamsMax[HNSWM] = strconv.Itoa(HNSWMaxM + 1)

	invalidParamsWithoutPQM := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         L2,
	}

	invalidParamsPQM := copyParams(validParams)
	invalidParamsPQM[PQM] = "NAN"

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidMatricParams, false},
		{invalidEfParamsMin, false},
		{invalidEfParamsMax, false},
		{invalidMParamsMin, false},
		{invalidMParamsMax, false},
		{invalidParamsWithoutPQM, false},
		{invalidParamsPQM, false},
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

	invalidEfParamsMin := copyParams(validParams)
	invalidEfParamsMin[EFConstruction] = strconv.Itoa(HNSWMinEfConstruction - 1)

	invalidEfParamsMax := copyParams(validParams)
	invalidEfParamsMax[EFConstruction] = strconv.Itoa(HNSWMaxEfConstruction + 1)

	invalidMParamsMin := copyParams(validParams)
	invalidMParamsMin[HNSWM] = strconv.Itoa(HNSWMinM - 1)

	invalidMParamsMax := copyParams(validParams)
	invalidMParamsMax[HNSWM] = strconv.Itoa(HNSWMaxM + 1)

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidEfParamsMin, false},
		{invalidEfParamsMax, false},
		{invalidMParamsMin, false},
		{invalidMParamsMax, false},
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

	invalidEdgeSizeParamsMin := copyParams(validParams)
	invalidEdgeSizeParamsMin[EdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidEdgeSizeParamsMax := copyParams(validParams)
	invalidEdgeSizeParamsMax[EdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidFPEdgeSizeParamsMin := copyParams(validParams)
	invalidFPEdgeSizeParamsMin[ForcedlyPrunedEdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidFPEdgeSizeParamsMax := copyParams(validParams)
	invalidFPEdgeSizeParamsMax[ForcedlyPrunedEdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidSPEdgeSizeParamsMin := copyParams(validParams)
	invalidSPEdgeSizeParamsMin[SelectivelyPrunedEdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidSPEdgeSizeParamsMax := copyParams(validParams)
	invalidSPEdgeSizeParamsMax[SelectivelyPrunedEdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidSPFPParams := copyParams(validParams)
	invalidSPFPParams[SelectivelyPrunedEdgeSize] = strconv.Itoa(60)
	invalidSPFPParams[ForcedlyPrunedEdgeSize] = strconv.Itoa(30)

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidEdgeSizeParamsMin, false},
		{invalidEdgeSizeParamsMax, false},
		{invalidFPEdgeSizeParamsMin, false},
		{invalidFPEdgeSizeParamsMax, false},
		{invalidSPEdgeSizeParamsMin, false},
		{invalidSPEdgeSizeParamsMax, false},
		{invalidSPFPParams, false},
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

	invalidEdgeSizeParamsMin := copyParams(validParams)
	invalidEdgeSizeParamsMin[EdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidEdgeSizeParamsMax := copyParams(validParams)
	invalidEdgeSizeParamsMax[EdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidOutEdgeSizeParamsMin := copyParams(validParams)
	invalidOutEdgeSizeParamsMin[OutgoingEdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidOutEdgeSizeParamsMax := copyParams(validParams)
	invalidOutEdgeSizeParamsMax[OutgoingEdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidInEdgeSizeParamsMin := copyParams(validParams)
	invalidInEdgeSizeParamsMin[IncomingEdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidInEdgeSizeParamsMax := copyParams(validParams)
	invalidInEdgeSizeParamsMax[IncomingEdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	cases := []struct {
		params map[string]string
		want   bool
	}{
		{validParams, true},
		{invalidEdgeSizeParamsMin, false},
		{invalidEdgeSizeParamsMax, false},
		{invalidOutEdgeSizeParamsMin, false},
		{invalidOutEdgeSizeParamsMax, false},
		{invalidInEdgeSizeParamsMin, false},
		{invalidInEdgeSizeParamsMax, false},
	}

	adapter := newNGTONNGConfAdapter()
	for _, test := range cases {
		if got := adapter.CheckTrain(test.params); got != test.want {
			t.Errorf("NGTONNGConfAdapter.CheckTrain(%v) = %v", test.params, test.want)
		}
	}
}
