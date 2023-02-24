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

package autoindex

import (
	"fmt"
	"strconv"
	"sync"

	"encoding/json"
)

type Parser struct {
	rw      sync.RWMutex
	methods sync.Map // map of level to Calculator
}

func NewParser() *Parser {
	return &Parser{
		methods: sync.Map{},
	}
}

func (p *Parser) InitFromJSONStr(value string) error {
	valueMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(value), &valueMap)
	if err != nil {
		return fmt.Errorf("init autoindex parser failed:%w", err)
	}
	return p.InitFromMap(valueMap)
}

func (p *Parser) InitFromMap(values map[string]interface{}) error {
	p.rw.Lock()
	defer p.rw.Unlock()
	p.methods = sync.Map{}
	var err error
	var cnt int
	for levelStr, value := range values {
		valueMap, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("parse params failed, wrong format")
		}
		var level int
		level, err = strconv.Atoi(levelStr)
		if err != nil {
			return fmt.Errorf("parse level failed, woring format")
		}

		var method Calculator
		method, err = newMethodNormalFromMap(valueMap)
		if err != nil {
			method, err = newMethodPieceWiseFromMap(valueMap)
		}
		if err != nil {
			return fmt.Errorf("parse method failed %w", err)
		}
		p.methods.Store(level, method)
		cnt++
	}
	if cnt == 0 {
		return fmt.Errorf("parse method failed: empty")
	}
	return nil
}

func (p *Parser) GetMethodByLevel(level int) (Calculator, bool) {
	m, ok := p.methods.Load(level)
	if !ok {
		return nil, false
	}
	return m.(Calculator), true
}

// GetSearchParamStrCalculator return a method which can calculate searchParams
func GetSearchCalculator(paramsStr string, level int) Calculator {
	parser := NewParser()
	err := parser.InitFromJSONStr(paramsStr)
	if err != nil {
		return nil
	}
	m, ok := parser.GetMethodByLevel(level)
	if !ok {
		return nil
	}
	return m
}
