// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package catalogservice

import (
	"errors"
	"fmt"
)

var ErrInvalidCatalogPathSegment = errors.New("invalid catalog path segment")

func ValidateCatalogPathSegment(kind string, value string) error {
	if value == "" || value == "." || value == ".." {
		return fmt.Errorf("%w: %s", ErrInvalidCatalogPathSegment, kind)
	}
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '-' || ch == '_' || ch == '.':
		default:
			return fmt.Errorf("%w: %s", ErrInvalidCatalogPathSegment, kind)
		}
	}
	return nil
}
