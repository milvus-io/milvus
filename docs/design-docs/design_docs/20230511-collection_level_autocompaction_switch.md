# MEP: Add collection level auto compaction config 

Current state: In Progress

ISSUE: [[Enhancement]: Support collection level config to disable auto-compaction #23993](https://github.com/milvus-io/milvus/issues/23993)

Keywords: Collection, Compaction, Config

Released: N/A

## Summary

Compaction has a config item to control whether auto-compaction is enabled or not. This configuration is global and impacts all collections in system.

In some scenarios, we might want to control the granularity of auto-compaction switch so that it could be achieved that:

- Disable auto-compaction during importing data to prevent rebuilt indexes
- Disable auto-compaction during some test cases to make system behavior stable

## Design

Add collection level attribute, attribute key is "collection.autocompaction.enabled"(see also pkg/common/common.go).

While handling all compaction signal, check collection level configuration:

- If not set, use global auto-compaction setting
- If config is valid, use collection level setting
- If config value is invalid, fallback to global setting


## How to change this setting

All collection-level attribute could be changed by `AlterCollection` API

## Test Plan

### Unit tests

Add unit tests for collection level auto compaction switch.

### E2E Tests

Change some case to disable collection auto compaction to rectify test case behavior.

### Integration Tests

- Add test to check auto compaction disabled
 
## References

None

