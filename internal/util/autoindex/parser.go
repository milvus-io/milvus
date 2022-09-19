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
	"sort"
	"strconv"
	"sync"

	"encoding/json"
)

type Parser struct {
	rw      sync.RWMutex
	methods map[int]Calculator // map of level to Calculator
}

func NewParser() *Parser {
	return &Parser{
		methods: make(map[int]Calculator),
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
	p.methods = make(map[int]Calculator)
	var err error
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

		methodIDV, ok := valueMap["methodID"]
		if !ok {
			return fmt.Errorf("parse method failed, methodID not specified")
		}
		methodID := parseMethodID(methodIDV)
		if methodID == -1 {
			return fmt.Errorf("parse method failed, methodID in wrong format")
		}
		var method Calculator
		switch methodID {
		case 1:
			method, err = parseMethodNormal(valueMap)
		case 2:
			method, err = parseMethodPieceWise(valueMap)
		}
		if method == nil {
			return fmt.Errorf("parse method failed %w", err)
		}
		p.methods[level] = method
	}
	if len(p.methods) == 0 {
		return fmt.Errorf("parse method failed: empty")
	}
	return nil
}

func (p *Parser) GetMethodByLevel(level int) (Calculator, error) {
	p.rw.RLock()
	defer p.rw.RUnlock()
	var levels []int
	for l := range p.methods {
		levels = append(levels, l)
	}
	sort.Slice(levels, func(i int, j int) bool {
		return levels[i] < levels[j]
	})
	lastLevel := -1
	// levels is sure to be non-empty
	minLevel := -1
	for i, l := range levels {
		if i == 0 {
			minLevel = l
		}
		if l > level {
			break
		}
		lastLevel = l
	}
	if lastLevel == -1 {
		lastLevel = minLevel
	}

	method, ok := p.methods[lastLevel]
	if !ok {
		return nil, fmt.Errorf("getMethodByLevel failed, level not exist")
	}
	return method, nil
}
