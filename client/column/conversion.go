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

package column

func (c *ColumnInt8) GetAsInt64(idx int) (int64, error) {
	v, err := c.ValueByIdx(idx)
	return int64(v), err
}

func (c *ColumnInt16) GetAsInt64(idx int) (int64, error) {
	v, err := c.ValueByIdx(idx)
	return int64(v), err
}

func (c *ColumnInt32) GetAsInt64(idx int) (int64, error) {
	v, err := c.ValueByIdx(idx)
	return int64(v), err
}

func (c *ColumnInt64) GetAsInt64(idx int) (int64, error) {
	return c.ValueByIdx(idx)
}

func (c *ColumnString) GetAsString(idx int) (string, error) {
	return c.ValueByIdx(idx)
}

func (c *ColumnFloat) GetAsDouble(idx int) (float64, error) {
	v, err := c.ValueByIdx(idx)
	return float64(v), err
}

func (c *ColumnDouble) GetAsDouble(idx int) (float64, error) {
	return c.ValueByIdx(idx)
}

func (c *ColumnBool) GetAsBool(idx int) (bool, error) {
	return c.ValueByIdx(idx)
}
