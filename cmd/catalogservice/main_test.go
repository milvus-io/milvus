// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util"
)

func TestDefaultCatalogServiceMetastoreTypeIsTiKV(t *testing.T) {
	require.Equal(t, util.MetaStoreTypeTiKV, defaultCatalogServiceMetastoreType())
}

func TestValidateCatalogServiceListenAddressRejectsWildcardWithoutOverride(t *testing.T) {
	require.Error(t, validateCatalogServiceListenAddress("0.0.0.0:19540", false))
	require.Error(t, validateCatalogServiceListenAddress(":19540", false))
	require.Error(t, validateCatalogServiceListenAddress("[::]:19540", false))
}

func TestValidateCatalogServiceListenAddressAllowsLoopback(t *testing.T) {
	require.NoError(t, validateCatalogServiceListenAddress("127.0.0.1:19540", false))
	require.NoError(t, validateCatalogServiceListenAddress("[::1]:19540", false))
	require.NoError(t, validateCatalogServiceListenAddress("localhost:19540", false))
}

func TestValidateCatalogServiceListenAddressAllowsExplicitOverride(t *testing.T) {
	require.NoError(t, validateCatalogServiceListenAddress("0.0.0.0:19540", true))
}

func TestNamespaceMetaRootMatchesMilvusMetaSubPath(t *testing.T) {
	root, err := namespaceMetaRoot("by-dev/catalog", "milvus1", "meta")
	require.NoError(t, err)
	require.Equal(t, "by-dev/catalog/milvus1/meta", root)

	root, err = namespaceMetaRoot("by-dev/catalog/", "milvus2", "/custom-meta/")
	require.NoError(t, err)
	require.Equal(t, "by-dev/catalog/milvus2/custom-meta", root)

	root, err = namespaceMetaRoot("by-dev/catalog", "milvus3", "")
	require.NoError(t, err)
	require.Equal(t, "by-dev/catalog/milvus3", root)
}

func TestNamespaceMetaRootRejectsUnsafeNamespace(t *testing.T) {
	for _, namespace := range []string{"", "../milvus1", "milvus/one", "milvus one"} {
		_, err := namespaceMetaRoot("by-dev/catalog", namespace, "meta")
		require.Error(t, err)
	}
}
