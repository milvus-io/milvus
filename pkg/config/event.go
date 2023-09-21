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

// Event Constant
const (
	UpdateType = "UPDATE"
	DeleteType = "DELETE"
	CreateType = "CREATE"
)

type Event struct {
	EventSource string
	EventType   string
	Key         string
	Value       string
	HasUpdated  bool
}

func newEvent(eventSource, eventType string, key string, value string) *Event {
	return &Event{
		EventSource: eventSource,
		EventType:   eventType,
		Key:         key,
		Value:       value,
		HasUpdated:  false,
	}
}

func PopulateEvents(source string, currentConfig, updatedConfig map[string]string) ([]*Event, error) {
	events := make([]*Event, 0)

	// generate create and update event
	for key, value := range updatedConfig {
		currentValue, ok := currentConfig[key]
		if !ok { // if new configuration introduced
			events = append(events, newEvent(source, CreateType, key, value))
		} else if currentValue != value {
			events = append(events, newEvent(source, UpdateType, key, value))
		}
	}

	// generate delete event
	for key, value := range currentConfig {
		_, ok := updatedConfig[key]
		if !ok { // when old config not present in new config
			events = append(events, newEvent(source, DeleteType, key, value))
		}
	}
	return events, nil
}
