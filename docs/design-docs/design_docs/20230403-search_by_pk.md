# MEP: Search By Primary Keys

Current state: Under Discussion

ISSUE: [[Feature]: Support to search by primary keys #23184](https://github.com/milvus-io/milvus/issues/23184)

Keywords: Search, ANN

Released: v2.3.0

## Summary

Support to search (ANNS) by the query vectors corresponding to the given primary keys.

## Motivation

For now, Milvus requires passing the query vectors to do anns, we have to fetch the vectors first if they are in the collection, which is complex and slow.

We need a way to do anns directly for the corresponding vectors of given primary keys, which should be more efficient.

## Public Interfaces

Add new field `primary_keys` in `SearchRequest`, all SDKs adds new method:
```golang
func SearchByPK(
    ctx context.Context, 
    collName string, 
    partitions []string,
	expr string, 
    outputFields []string, 
    primaryKeys []entity.PrimaryKey, 
    vectorField string, 
    metricType entity.MetricType, 
    topK int, 
    sp entity.SearchParam, 
    opts ...SearchQueryOptionFunc,
) ([]SearchResult, error)
```

## Design Details

Proxy fetches the vectors by primary keys from QueryNodes first, and then search with these vectors.

For better performance, we will add a new RPC interface for QueryNode:
```proto
rpc Fetch(FetchRequest) returns (FetchResponse) {}
```
which only supports to fetch by primary keys, this will be more efficient than the present `Query` interface.


## Compatibility, Deprecation, and Migration Plan

None

## Test Plan

### Unit Tests

- Test for fetching in segcore
- Test for fetching in QueryNode
- Test for searching by primary keys in Proxy


### E2E Tests
|                  Test Cases                  |         Expected Behavior          |
| :------------------------------------------: | :--------------------------------: |
|      search by non-existed primary keys      |            report error            |
|        search by existed primary keys        | return topk results for each query |
| all present tests cases of search by vectors |              the same              |

## Rejected Alternatives

Implement this without adding the `Fetch` interface, using `Query` to retrieve the vectors and then search by vectors.

## References

None
