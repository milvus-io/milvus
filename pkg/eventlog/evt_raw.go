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

package eventlog

// rawEvt implement `Evt` interface with plain event msg.
type rawEvt struct {
	level Level
	tp    int32
	data  []byte
}

func (l *rawEvt) Level() Level {
	return l.level
}

func (l *rawEvt) Type() int32 {
	return l.tp
}

func (l *rawEvt) Raw() []byte {
	return l.data
}

func NewRawEvt(level Level, data string) Evt {
	return &rawEvt{
		level: level,
		data:  []byte(data),
	}
}
