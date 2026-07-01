/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import "github.com/milvus-io/milvus/pkg/v3/util/merr"

// PruneDataFrame returns a DataFrame containing keepNonSystem plus retained system columns.
// If no column is dropped, the original DataFrame is returned unchanged.
func PruneDataFrame(df *DataFrame, keepNonSystem ColumnSet, policy SystemColumnPolicy) (*DataFrame, error) {
	if df == nil {
		return nil, merr.WrapErrServiceInternal("dataframe is nil")
	}

	policy = normalizeSystemColumnPolicy(policy)
	colNames := df.ColumnNames()
	keepNames := make([]string, 0, len(colNames))
	dropped := false
	for _, name := range colNames {
		keep := keepNonSystem.Contains(name)
		if policy.KeepAllSystemColumns && IsFunctionChainSystemName(name) {
			keep = true
		}
		if keep {
			keepNames = append(keepNames, name)
		} else {
			dropped = true
		}
	}

	if !dropped {
		return df, nil
	}

	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(df.chunkSizes)
	builder.CopyAllMetadata(df)

	for _, name := range keepNames {
		if err := builder.AddColumnFrom(df, name); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}
