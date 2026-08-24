// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package inspector

import (
	"context"
)

func (r ObjectReader) InspectTextLog(ctx context.Context, object Object, expectedEZID int64, artifactVersion int32) error {
	raw, err := r.Read(ctx, object)
	if err != nil {
		return err
	}
	if artifactVersion == 2 {
		return InspectV2(raw, expectedEZID)
	}
	return InspectV3(raw, expectedEZID)
}
