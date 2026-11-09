// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0.

package importv3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReshardProgressSourceProgresses(t *testing.T) {
	p := NewReshardProgress()
	p.AddHashed(1, 10)
	p.AddHashed(1, 5)
	p.AddHashed(2, 7)
	p.AddHashed(2, 0)  // ignored
	p.AddHashed(3, -1) // ignored

	got := make(map[int64]int64)
	for _, sp := range p.SourceProgresses() {
		got[sp.GetFileId()] = sp.GetHashedRows()
	}
	require.Equal(t, map[int64]int64{1: 15, 2: 7}, got)

	require.Nil(t, (*ReshardProgress)(nil).SourceProgresses())
}
