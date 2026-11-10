// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package policy

import "time"

// ResetSingleCompactionAdmitterForTest replaces the shared admitter with a
// fresh one, so a test starts from a full bucket and empty cursors, and
// returns it.
func ResetSingleCompactionAdmitterForTest() *SingleCompactionAdmitter {
	GetSingleCompactionAdmitter()
	globalSingleCompactionAdmitter = newSingleCompactionAdmitter(time.Now)
	return globalSingleCompactionAdmitter
}

// TokensForTest returns the tokens left in the bucket.
func (a *SingleCompactionAdmitter) TokensForTest() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tokens
}

// SetTokensForTest sets the tokens left in the bucket.
func (a *SingleCompactionAdmitter) SetTokensForTest(tokens float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokens = tokens
}

// ServedForTest returns how many tokens a producer has taken since the budget
// became contested.
func (a *SingleCompactionAdmitter) ServedForTest(producer string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.served[producer]
}

// SetServedForTest sets how many tokens a producer has taken since the budget
// became contested.
func (a *SingleCompactionAdmitter) SetServedForTest(producer string, n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.served[producer] = n
}

// NewSingleCompactionAdmitterForTest returns a fresh admitter that is not the
// shared one.
func NewSingleCompactionAdmitterForTest() *SingleCompactionAdmitter {
	return newSingleCompactionAdmitter(time.Now)
}
