// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package config

import (
	"strings"
)

type EventDispatcher struct {
	registry  map[string][]EventHandler
	keyPrefix []string
}

func NewEventDispatcher() *EventDispatcher {
	return &EventDispatcher{
		registry:  make(map[string][]EventHandler),
		keyPrefix: make([]string, 0),
	}
}

func (ed *EventDispatcher) Get(key string) []EventHandler {
	return ed.registry[formatKey(key)]
}

func (ed *EventDispatcher) Dispatch(event *Event) {
	var hs []EventHandler
	realKey := formatKey(event.Key)
	hs, ok := ed.registry[realKey]
	if !ok {
		for _, v := range ed.keyPrefix {
			if strings.HasPrefix(realKey, v) {
				if _, exist := ed.registry[v]; exist {
					hs = append(hs, ed.registry[v]...)
				}
			}
		}
	}
	for _, h := range hs {
		h.OnEvent(event)
	}
}

// register a handler to watch specific config changed
func (ed *EventDispatcher) Register(key string, handler EventHandler) {
	key = formatKey(key)
	v, ok := ed.registry[key]
	if ok {
		ed.registry[key] = append(v, handler)
	} else {
		ed.registry[key] = []EventHandler{handler}
	}
}

// register a handler to watch specific config changed
func (ed *EventDispatcher) RegisterForKeyPrefix(keyPrefix string, handler EventHandler) {
	keyPrefix = formatKey(keyPrefix)
	v, ok := ed.registry[keyPrefix]
	if ok {
		ed.registry[keyPrefix] = append(v, handler)
	} else {
		ed.registry[keyPrefix] = []EventHandler{handler}
	}
	ed.keyPrefix = append(ed.keyPrefix, keyPrefix)
}

func (ed *EventDispatcher) Unregister(key string, handler EventHandler) {
	key = formatKey(key)
	v, ok := ed.registry[key]
	if !ok {
		return
	}
	newGroup := make([]EventHandler, 0)
	for _, eh := range v {
		if eh.GetIdentifier() == handler.GetIdentifier() {
			continue
		}
		newGroup = append(newGroup, eh)
	}
	ed.registry[key] = newGroup
}
