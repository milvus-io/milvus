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

package proxy

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
)

// CollectionMetricLabels returns the db_name and collection_name label values
// for a finished request that named dbName and collectionName.
//
// The proxy deletes a collection's series when the collection is dropped and a
// database's when the database is dropped (metrics.CleanupProxyCollectionMetrics,
// metrics.CleanupProxyDBMetrics). A name that does not denote an existing
// collection or database is never dropped, so a series labeled with it is
// never deleted - and the name comes from the request, so a client could mint
// series without bound by varying it. A name is therefore kept only while it
// denotes something that exists, and is recorded as "" otherwise:
//
//   - a collection name, collection or alias, is kept if it is in the meta
//     cache when the request finishes - every request that resolves a
//     collection caches it - or the request created it;
//   - a database name is kept if the database is in the meta cache, or the
//     request created it.
//
// The cache is read only, so this never adds an RPC. A request that fails
// before it resolves the collection, on a proxy whose cache has not seen the
// collection yet, is recorded under "".
func CollectionMetricLabels(cache Cache, method, dbName, collectionName string, resp any, err error) (string, string) {
	status, _ := requestutil.ParseMetricLabel(resp, err)
	return collectionMetricLabels(cache, method, status, dbName, collectionName)
}

// collectionCreators are the methods whose success creates the collection they
// name.
var collectionCreators = map[string]bool{
	"CreateCollection":        true,
	"RestoreSnapshot":         true,
	"RestoreExternalSnapshot": true,
}

// collectionMetricLabels is CollectionMetricLabels for a request whose outcome
// is already the status label it is counted under.
func collectionMetricLabels(cache Cache, method, status, dbName, collectionName string) (string, string) {
	if cache == nil {
		return "", ""
	}
	succeeded := status == metrics.SuccessLabel
	if collectionName != "" {
		if cache.HasCachedCollection(dbName, collectionName) || (succeeded && collectionCreators[method]) {
			return dbName, collectionName
		}
		collectionName = ""
	}
	if dbName != "" && !cache.HasCachedDatabase(dbName) && (!succeeded || method != "CreateDatabase") {
		dbName = ""
	}
	return dbName, collectionName
}

// functionCallLabels returns the ProxyFunctionCall label values of a request
// to method that named dbName and collectionName, counted under status and
// cause, with the names kept as CollectionMetricLabels keeps them.
func (node *Proxy) functionCallLabels(method, status, cause, dbName, collectionName string) []string {
	dbName, collectionName = collectionMetricLabels(node.GetMetaCache(), method, status, dbName, collectionName)
	return []string{paramtable.GetStringNodeID(), method, status, cause, dbName, collectionName}
}

// observeReceivedNQ counts the nq a search or query received. It runs when the
// request finishes, deferred from its start, so the collection it named has
// been resolved by then and CollectionMetricLabels can tell whether it exists.
func (node *Proxy) observeReceivedNQ(queryType, dbName, collectionName string, nq float64) {
	dbName, collectionName = CollectionMetricLabels(node.GetMetaCache(), "", dbName, collectionName, nil, nil)
	metrics.ProxyReceivedNQ.WithLabelValues(
		paramtable.GetStringNodeID(),
		queryType,
		dbName,
		collectionName,
	).Add(nq)
}

// addQueryRate counts a query in the DQL query rate, and in its collection's
// sub-rate while the collection exists. A sub-rate is allocated per collection
// it is first added for and released only when the collection is dropped
// (DeregisterSubLabel), so one added for a name that is not a collection would
// never be released. It runs when the query finishes, deferred from its start,
// so the collection the query named has been resolved by then.
func (node *Proxy) addQueryRate(request *milvuspb.QueryRequest) {
	subLabel := ""
	if cache := node.GetMetaCache(); cache != nil && cache.HasCachedCollection(request.GetDbName(), request.GetCollectionName()) {
		subLabel = GetCollectionRateSubLabel(request)
	}
	rateCol.Add(internalpb.RateType_DQLQuery.String(), 1, subLabel)
}
