# index-advisor

`index-advisor` reads Milvus proxy access logs and lists, for each scalar field
that queries and searches filter on, the index candidates to validate. Each
candidate comes with its evidence and a before/after validation plan. Indexed
fields that no logged request filters on are listed for review. The tool
runs offline on files you provide and never connects to Milvus. It is advisory
only. It never creates, alters or drops an index, and it does not promise a
speedup.

The design is described in
[docs/design-docs/design_docs/20261016-scalar-index-advisor.md](../../../docs/design-docs/design_docs/20261016-scalar-index-advisor.md).

## Quickstart

Run from the repository root on the example inputs in `testdata/`:

```bash
go run ./cmd/tools/index-advisor \
  --access-log cmd/tools/index-advisor/testdata/access_log.txt \
  --snapshot cmd/tools/index-advisor/testdata/snapshot.json \
  --field-stats cmd/tools/index-advisor/testdata/field_stats.json \
  --output advice.json
```

`advice.json` is identical to `testdata/advice_with_stats.json`. Without
`--field-stats`, the output is `testdata/advice.json`.

To advise on your own deployment:

1. Enable the proxy access log, for example in `user.yaml` (see
   [Configure Access Logs](https://milvus.io/docs/configure_access_logs.md)):

   ```yaml
   proxy:
     accessLog:
       enable: true
       filename: access.log
       localPath: /var/lib/milvus/access_log
   ```

2. After the workload has run, describe the collections and their indexes in a
   snapshot file, for example with the script in
   [Preparing the snapshot](#preparing-the-snapshot):

   ```bash
   python snapshot.py products > snapshot.json
   ```

3. Run the advisor on the log files:

   ```bash
   go run ./cmd/tools/index-advisor \
     --access-log access.log --snapshot snapshot.json --output advice.json
   ```

## Command

```text
index-advisor --access-log FILE [--access-log FILE ...] --snapshot FILE
              [--field-stats FILE] [--milvus-config FILE] [--output FILE]
```

| Flag | Meaning |
|---|---|
| `--access-log` | Proxy access log file. Required. Repeat it for rotated files |
| `--snapshot` | Schemas and existing indexes, see [Snapshot](#snapshot). Required |
| `--field-stats` | Optional field statistics, see [Field statistics](#field-statistics) |
| `--milvus-config` | Optional `milvus.yaml` or `user.yaml` whose `proxy.accessLog.formatters` override the defaults |
| `--output` | Output file. The report goes to stdout when it is omitted |

An invalid flag or input file prints an error to stderr and exits with status
1. A key that the snapshot or statistics format does not define is ignored,
and a warning on stderr names its path, for example:

```text
index-advisor: warning: snapshot file snapshot.json: ignoring unknown key collections[*].fields[*].is_primary
```

`[*]` stands for any array element or map entry. Check these warnings, because
a misnamed key is read as absent.

## Inputs

### Access log

The advisor reads logs written with the default formatters of
`configs/milvus.yaml`. If your deployment changes `proxy.accessLog.formatters`,
pass its `milvus.yaml` or `user.yaml` with `--milvus-config`. A custom format is
supported when:

- each record starts with `[$time_now] [ACCESS] `, as in the defaults. The
  rest of the format can be reordered, and placeholders can be added or
  removed.
- every formatter logs `$method_name`, and the formatters for queries and
  searches also log `$collection_name` and `$method_expr`. `$database_name`,
  `$method_status` and `$time_cost` are used when present. Without
  `$database_name`, requests count for database `default`.

A file in which no record starts this way, records that match no formatter,
and query or search formatters without the placeholders above are named in
warnings on stderr. Example: `testdata/access_log.txt`.

Every record is either analyzed or counted under one skip reason. The report's
`records_total` is `records_analyzed` plus all skip counts except
`orphan_line`, which counts lines rather than records:

| Reason | Records |
|---|---|
| `orphan_line` | Lines before the first record header |
| `unmatched_format` | Records that match no formatter |
| `not_a_filter_request` | Methods whose formatter logs no expression, such as Insert |
| `method_out_of_scope` | Methods that log an expression but are not a query or search, such as Delete |
| `hybrid_search_unsupported` | Hybrid searches, whose sub-request filters cannot be split reliably |
| `request_failed` | Requests whose logged status is not `Successful` |
| `no_filter` | Queries and searches without a filter |
| `unknown_collection` | Requests on a collection that is not in the snapshot, by name or alias |
| `expression_parse_error` | Filters that Milvus's expression parser rejects against the snapshot schema |

### Snapshot

The snapshot lists the schema and the existing indexes of each collection in
the logs. It is unrelated to the Milvus
[Snapshot](https://milvus.io/docs/manage-snapshots.md) feature, which backs up
collection data. Example: `testdata/snapshot.json`.

```json
{
  "format_version": 1,
  "collections": [{
    "db": "default",
    "name": "products",
    "aliases": ["products_live"],
    "enable_dynamic_field": true,
    "fields": [
      {"name": "id", "data_type": "INT64", "is_primary_key": true},
      {"name": "vec", "data_type": "FLOAT_VECTOR"},
      {"name": "city", "data_type": "VARCHAR"},
      {"name": "tags", "data_type": "ARRAY", "element_type": "VARCHAR"},
      {"name": "meta", "data_type": "JSON"}
    ],
    "indexes": [
      {"index_name": "city", "field_name": "city", "index_type": "INVERTED"},
      {"index_name": "meta_brand", "field_name": "meta", "index_type": "INVERTED",
       "params": {"json_path": "meta[\"brand\"]", "json_cast_type": "varchar"}}
    ]
  }]
}
```

| Key | Required | Meaning |
|---|---|---|
| `format_version` | yes | `1` |
| `collections[]` | yes | At least one collection |
| `collections[].db` | no | Database name. Defaults to `default` |
| `collections[].name` | yes | Collection name, as the access log writes it |
| `collections[].aliases` | no | The collection's aliases, as `DescribeCollection` reports them. Requests through an alias count for the collection |
| `collections[].enable_dynamic_field` | no | Whether the collection has a dynamic field. Do not list `$meta` in `fields` |
| `fields[]` | yes | Every field in the schema, including vector fields |
| `fields[].name` | yes | Field name |
| `fields[].data_type` | yes | A Milvus data type name, matched without case or underscores, so `VarChar` and `VARCHAR` are the same |
| `fields[].element_type` | for arrays | The element type of an `ARRAY` field |
| `fields[].is_primary_key` | no | `true` for the primary key |
| `indexes[]` | no | Every index on the collection, as `DescribeIndex` reports it |
| `indexes[].index_name`, `field_name`, `index_type` | yes | As reported. A dynamic-key index has the key as its `field_name` |
| `indexes[].params.json_path` | for JSON paths | The `json_path` the index was created with. Without it, the index counts for the whole field |
| `indexes[].params.json_cast_type` | no | The `json_cast_type` of a JSON index, which tells the filters it serves. Other params are allowed and not read |

The keys follow the field names of `schemapb.FieldSchema`. SDKs and the
RESTful API rename some of them. pymilvus reports `is_primary` and `type`, and
the RESTful API reports `primaryKey`.

#### Preparing the snapshot

Any way of producing this JSON works, whether an SDK, the RESTful API or a
hand-written file. The advisor cannot detect a field missing from `fields`. In
a collection with a dynamic field, such a field is read as a dynamic key.

The script below is one suggestion, tested with pymilvus 3.0.2 against Milvus
v3.0.3. It needs read access to the collections and their indexes. Adapt the
connection, the database and the key mapping to your environment:

```python
# snapshot.py: writes an index-advisor snapshot for collections in one
# database. Pass collection names as arguments, or none for every collection.
import json
import sys

from pymilvus import MilvusClient

DB = "default"
client = MilvusClient(uri="http://localhost:19530", db_name=DB)  # add token="user:password" if needed
names = sys.argv[1:] or client.list_collections()


def field(f):
    out = {"name": f["name"], "data_type": f["type"].name}
    if "element_type" in f:
        out["element_type"] = f["element_type"].name
    if f.get("is_primary"):
        out["is_primary_key"] = True
    return out


def index(collection, name):
    d = client.describe_index(collection, name)
    out = {"index_name": name, "field_name": d["field_name"], "index_type": d["index_type"]}
    nested = d["params"] if isinstance(d.get("params"), dict) else {}  # params of an index created with nested params
    params = {k: v for k, v in {**nested, **d}.items() if k in ("json_path", "json_cast_type")}
    if params:
        out["params"] = params
    return out


collections = []
for name in names:
    d = client.describe_collection(name)
    collections.append({
        "db": DB,
        "name": name,
        "aliases": d.get("aliases", []),
        "enable_dynamic_field": d["enable_dynamic_field"],
        "fields": [field(f) for f in d["fields"]],
        "indexes": [index(name, i) for i in client.list_indexes(name)],
    })
json.dump({"format_version": 1, "collections": collections}, sys.stdout, indent=2)
print()
```

When producing the file another way:

- `describe_collection` does not list the dynamic field. Set
  `enable_dynamic_field` instead.
- The RESTful `collections/describe` response lists only the index of a vector
  field. Use `indexes/list` and `indexes/describe` for every index.
- Take the snapshot with the same indexes that existed when the logs were
  written. An index created or dropped in between changes what
  `review_existing` means.

### Field statistics

Milvus does not report per-field statistics, so they come from an optional
file. `source`, `collected_at` (RFC 3339) and `scope` are required and copied
into the report. Fields are keyed by name or JSON path. A dynamic key can be
written as `color` or `$meta["color"]`. A statistic the file does not give is
reported as `"unknown"`. Example: `testdata/field_stats.json`.

| Statistic | Used for |
|---|---|
| `distinct_count` | Deciding whether BITMAP is a candidate, on integer and VARCHAR fields and arrays of them, filtered by `==`, `in` or `array_contains`. For an array field, count distinct elements |
| `row_count`, `null_fraction` | Shown in the report as context for the validation |

Only the fields you want BITMAP evaluated for need a `distinct_count`. The
numbers can be gathered with queries, for example:

- `row_count`: a `count(*)` query with an empty filter.
- `null_fraction`: a `count(*)` query with the filter `field is null`, divided
  by `row_count`.
- `distinct_count` for integer and VARCHAR fields: a query that groups by the
  field (`group_by_fields`) with `count(*)` as output returns one row per
  distinct value, within the query result limit.
- `distinct_count` for arrays, which cannot be grouped by: read the values with
  a query iterator, or from an export of the data, and count the distinct
  elements.

These queries are written to the access log too. Copy the logs before
gathering statistics, so that the queries are not analyzed as workload.

```json
{
  "format_version": 1,
  "source": "SELECT COUNT(DISTINCT ...) on a full export",
  "collected_at": "2026-10-15T00:00:00Z",
  "scope": "default.products, all rows",
  "collections": [{
    "db": "default",
    "name": "products",
    "row_count": 5000,
    "fields": {
      "city": {"distinct_count": 5, "null_fraction": 0}
    }
  }]
}
```

## Output

Examples: `testdata/advice.json` and `testdata/advice_with_stats.json`.

- `inputs`: log file names, record counts, skip counts, and the provenance of
  the field statistics.
- `fingerprints`: each query shape with its count, methods and logged latency.
- `recommendations`: one entry per filtered field, and per indexed scalar field
  that no analyzed request filters on. Entries are ranked by action, then by
  the number of requests filtering on the field.
- `notes`: fixed reminders about how to read the report.

| Action | Meaning |
|---|---|
| `validate_candidates` | The field has no index. Validate the candidates |
| `review_existing` | The field has an index, which does not show that any request used it. Validate it and compare it with the candidates |
| `no_action` | No candidate applies. The entry's `notes` say why |

A recommendation entry, abridged from `testdata/advice_with_stats.json`:

```json
{
  "column": "city",
  "action": "review_existing",
  "observed": {"predicates": 16, "op_classes": {"eq": 10, "in": 6}},
  "existing_indexes": [{"index_name": "city", "index_type": "INVERTED"}],
  "candidates": [
    {"index_type": "AUTOINDEX", "role": "baseline", "field_name": "city",
     "evidence": ["this column is filtered by 16 analyzed requests", "..."]},
    {"index_type": "BITMAP", "role": "alternative", "field_name": "city",
     "evidence": ["distinct_count 5 is below 500, where BITMAP is documented to be most effective", "..."]}
  ],
  "statistics": {"row_count": 5000, "distinct_count": 5, "null_fraction": 0},
  "validation_plan": ["On non-production copies of default.products, one with the existing index and one without, replay ...", "..."]
}
```

A candidate's `field_name` and `params` are the arguments for `create_index`.
A JSON path carries `json_path` and the `json_cast_type` inferred from the
literals compared with it. A key in the dynamic field is indexed under its
top-level key, with `json_path` written as `$meta["key"]`.

## How candidates are chosen

| Candidate | When it is listed |
|---|---|
| AUTOINDEX | The baseline for any field on which Milvus accepts it |
| BITMAP | `distinct_count` below 500, on an integer or VARCHAR field, or an array of them, filtered by `==`, `in` or `array_contains` |
| NGRAM | Infix, suffix or pattern `LIKE`, when every fixed part of every observed `LIKE` filter is at least `min_gram` characters long |
| INVERTED | `json_contains` on a JSON path that holds an array, with the array `json_cast_type` (`ARRAY_VARCHAR`, `ARRAY_DOUBLE` or `ARRAY_BOOL`) its literals give. AUTOINDEX does not accept array casts |

A field gets no candidate when it is the primary key, when a JSON path is
compared with literals of several types, or when no scalar index serves any
filter on it, as with field-to-field comparisons, arithmetic, comparisons on
one array element and `text_match`. An existing index type is not proposed
again. An existing index with `json_cast_type` `JSON` counts for the paths
beneath its own. When an alternative cannot be evaluated, for example because
`distinct_count` is unknown, a note says so.

## Privacy

The report contains fingerprints, never raw expressions. Literals, user names,
client addresses and trace IDs are left out, and log files appear by base name
only. The report does name databases, collections, fields and JSON keys, and
carries the provenance text of the field statistics.

## Limitations

- Filters on struct array fields are not analyzed reliably, because the
  snapshot cannot describe these fields. Such filters fail to parse or, with a
  dynamic field, are read as dynamic keys.

## Tests

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./cmd/tools/index-advisor/...
```

After an intended change to the output, regenerate the golden files with
`-run TestAdviseGolden -update` and review the diff.
