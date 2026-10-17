# Knowhere ownership of vector index parameters

Status: implementation design, 2026-10-14.

## Objective

Knowhere owns vector algorithm parameter names, defaults, validation, stage
semantics, and derived values. Milvus passes opaque algorithm parameters through
its existing interfaces and supplies execution facts and resource budgets.
Adding an algorithm parameter must not require a Milvus parameter declaration,
YAML default, algorithm-specific branch, or validation whitelist.

The implementation consists of two PRs, one in Knowhere and one in Milvus.
The Milvus PR depends on [Knowhere PR #1881](https://github.com/zilliztech/knowhere/pull/1881)
and includes this design document.
Tests ship with their respective implementation PRs.

Compatibility work is out of scope. Do not implement old-config
migration, mixed-version negotiation, deprecation aliases, metadata migration,
or parallel legacy execution paths. This does not require changing the index
file format or gratuitously renaming current public request parameters.

## Baseline evidence

- Milvus's generic vector checker already calls
  `ValidateIndexParams -> IndexStaticFaced::ConfigCheck`.
- Milvus base `73d056c78d` pins Knowhere `16e873b1`. Its sparse config already validates
  `inverted_index_algo`; the corresponding Go whitelist duplicates it. The Go
  comment saying Knowhere no longer validates this parameter is stale. Removing
  that whitelist also adopts Knowhere's existing acceptance of explicit empty
  strings and case-insensitive algorithm names; unknown names still fail.
- DiskANN config already derives PQ and cache budgets during TRAIN. The load
  path still needs an audit of derived budgets and their actual consumers.
- The pinned DiskANN Deserialize implementation uses the global Knowhere search
  pool size when loading. The Milvus translation to `num_threads` has no consumer
  in that implementation. Do not mechanically transplant obsolete settings.
- At Milvus base `73d056c78d`, DiskANN parameter handling remains in Proxy,
  DataCoord, DataNode, QueryNode, and the C++ vector builder/loader/reader.
  The former VectorDiskIndex has been split into those three responsibilities.
- Milvus has both generic `knowhere.<type>.<stage>` maps and a separate
  `common.DiskIndex.*` / `autoIndex.params.extra` parameter interpretation layer.

## Selected contract

1. Algorithm defaults live only in Knowhere Config definitions. Move defaults
   currently shipped as Milvus algorithm defaults to these definitions; use
   existing Knowhere definitions for other parameters. Record the selected
   values in Knowhere tests instead of duplicating them in Milvus.
2. Missing parameters reach Knowhere as missing. Explicit zero, false, and empty
   values retain their meaning and are not converted into missing values by
   Milvus. Knowhere determines which values are valid.
3. Retain the existing generic configuration mechanism for explicitly supplied
   deployment overrides, but remove shipped DiskANN/AISAQ defaults and their
   parameter-specific documentation switch from Milvus. Ordinary parameter
   precedence is explicit operation/index parameters, then generic deployment
   overrides, then Knowhere defaults. AutoIndex can still select an index type
   and an opaque parameter map; it must not parse DiskANN-specific ratio bundles.
   Keep existing AutoIndex selection policy: cloud AutoIndex supplies the
   authoritative build map while preserving the user metric, and non-cloud
   AUTOINDEX accepts only a user metric. Parameter precedence applies after
   this selection; the selected opaque map is an explicit index choice. This
   work does not introduce per-request cloud AutoIndex tuning.
   Explicit deployment build settings continue to be captured in index metadata
   at creation. This keeps build artifacts and fallback queries consistent (for
   example MinHash). They are index choices after capture, not live load defaults.
4. Build/load parameters attached to an index continue through the existing
   metadata path. Search controls are interpreted during search. Do not add
   implicit inheritance from CreateIndex into Search, a new parameter snapshot
   format, or a new online AlterIndex capability for this work.
5. Use existing Knowhere config, thread-pool, and resource interfaces. Milvus
   supplies schema/data facts and its resource budgets through generic paths.
   Knowhere owns conversions into algorithm-specific values. Runtime resource
   information must not depend on enabling YAML algorithm overrides.
6. Static validation checks supplied values and combinations without requiring
   worker-local execution resources. Resource-dependent resolution happens at
   Build/Deserialize, where the required data and execution environment exist.
7. Keep Milvus schema/function validation and generic capability checks. Remove
   algorithm parameter knowledge from both Go and the segcore adapter. Disk/GPU
   capability plumbing and file management remain generic execution concerns;
   this project is not a rewrite of every index wrapper or the scheduler.

## Coverage of Milvus PR #50046

Reference: [PR #50046](https://github.com/milvus-io/milvus/pull/50046), reviewed
at `f58309e5aad803076b1ff9e82adcf029ac5ef72c`, and its linked
[issue #49502](https://github.com/milvus-io/milvus/issues/49502).
The requested user capability is different configurations per index through
the API, without editing global defaults or restarting Milvus. Both PRs must
preserve this capability; removal of compatibility work is not permission to
drop working parameter controls.

| Capability | Required result in this design |
| --- | --- |
| Per-index graph and PQ settings | Explicit `max_degree`, `search_list_size`, and `pq_code_budget_gb_ratio` reach Knowhere without a Milvus override. |
| Disk PQ configuration | `disk_pq_code_budget_gb_ratio` is resolved inside Knowhere using the actual vector representation; explicit zero disables disk PQ where supported. |
| AISAQ-specific settings | Cover `inline_pq`, `rearrange`, `num_entry_points`, `pq_cache_size`, and `pq_read_page_cache_size` at their actual build/load/search consumers. |
| Per-request search controls | Cover `search_list`, `beamwidth`, and AISAQ `vectors_beamwidth`, including omitted values and range/iterator variants where supported. |
| Index-specific cache budgets | Explicit `search_cache_budget_gb_ratio` survives metadata, full load, and Reopen, and changes the actual cache population. |
| Deployment load overrides | For an index without an explicit load setting, changing a generic load-stage override takes effect on subsequent release/load without rebuilding the index. An explicit index setting still wins. |
| AutoIndex | The selected algorithm and opaque parameter map receive the same behavior, without a special DiskANN/AISAQ ratio parser. |
| Default-only operation | Both algorithms build/load/search with no Milvus algorithm configuration; defaults and validation live in Knowhere. |

Record AISAQ defaults introduced by #50046 in Knowhere rather than recreating
its YAML entries: disk-PQ ratio `0.25`, `inline_pq=-1`, `num_entry_points=100`,
`pq_cache_size=0`, `rearrange=true`, search-cache ratio `0`, and
`pq_read_page_cache_size=0`. DiskANN retains its distinct defaults. Verify these
along with the existing graph/PQ defaults in Knowhere tests.

Both algorithms validate `max_degree` against the backend graph limit, currently
`[1, 512]`. DiskANN previously accepted requests up to 2048 even though its loader
already rejected artifact degrees above 512; partitioned builds can write the
requested degree into that artifact metadata. Reject these requests before
building. External e2e fixtures that expect a successful Build at 2048 must adopt
the supported boundary; existing native tests cover accepting 512 and rejecting 513.

Deployment load/search overrides are merged at their execution stages, not
persisted as explicit index choices during CreateIndex. In particular, changing
a load-stage cache override must not be defeated by an earlier merge of shipped
algorithm defaults. An explicit setting captured from a deployment build map
remains an index choice and takes precedence, just like an API index setting.
Test this through the real Proxy/metadata/load path. No automatic mutation of an
already loaded index or new online AlterIndex API is implied.

Keep the original index parameter map separate from runtime load additions in
the existing in-process Go/segcore load payloads. QueryCoord metadata stays
unchanged. Full load, Reopen, and admission estimation prepare these payloads
through the same generic helper. The native loader merges missing keys for
execution, while static raw-data capability uses the original index parameters.
Otherwise, a load-only setting could misdescribe an existing artifact and make
the planner discard its required field data before the artifact is opened. This
separation contains no algorithm parameter list and does not persist a new
default snapshot.

The coverage target is the useful behavior, not the old configuration spelling
or helper implementation. Obsolete thread-ratio translations can be removed;
live concurrency controls use existing Knowhere controls and pools. Per-request
beamwidth remains configurable even when Milvus's beamwidth-ratio conversion is
removed. This matrix is an implementation acceptance requirement, not a claim
that the new implementation has already passed these checks.

## PR 1: Knowhere completes parameter ownership

Suggested title: `enhance: centralize disk index parameter defaults and resolution`

Changes:

- Consolidate DiskANN/AISAQ defaults and validation in their Config definitions,
  using inheritance for genuinely shared rules and explicit AISAQ overrides.
- Complete resolution for Build, Deserialize, Search, RangeSearch, and iterator
  stages wherever the affected parameter is consumed. Move any necessary public
  search-parameter translation from Milvus to Knowhere.
- Compute PQ budgets and disk-PQ dimensions from the actual vector data type,
  dimension, and vector count. For vector arrays, use the underlying vector
  count rather than assuming the collection row count is the vector count.
- Resolve load cache budgets inside Knowhere. Cover the path that loads a cached
  node list as well as the path that generates one; accepting a budget in Config
  is insufficient if Deserialize ignores it.
- Use existing generic build-thread controls and Knowhere pools. Delete dead
  Milvus-only thread/beamwidth ratio concepts rather than introducing redundant
  configuration aliases in Knowhere. If a live control needs a resource-based
  default, derive it inside Knowhere from the existing execution context.
- Ensure explicit search beamwidth controls reach Search, RangeSearch, and
  iterators without Milvus-side overrides.
- Verify that the existing sparse validation rejects invalid algorithms through
  ConfigCheck. Add missing coverage, not another whitelist implementation.
- Keep capabilities consistent with the actual disk representation. A disk-PQ
  index does not contain original vectors: static capability uses effective build
  settings, while loaded capability uses artifact metadata. Direct raw-vector
  retrieval and exact-distance APIs reject compressed artifacts. Milvus uses its
  existing field-data path to return original vectors. Reject disk PQ with native
  DiskANN embedding-list exact reranking during build validation; uncompressed
  array indexes remain supported.
- Use disk-PQ distance evaluation in both graph search and filtered brute-force
  refinement. Neither cached nor uncached candidates may be interpreted as raw
  vector values when the artifact stores PQ codes.

Primary files: `src/index/diskann/diskann_config.h`, `aisaq_config.h`, disk index
implementations, `include/knowhere/config.h`, and existing config/index tests.

Acceptance:

- Direct Knowhere Build -> serialize -> load -> Search succeeds with only the
  required user inputs and execution context, without Milvus's Fill/Set helpers.
- Two explicit parameter sets yield different values at actual algorithm
  consumers. Check graph/build settings, disk PQ, load cache, and search settings
  separately; do not use a generic successful search as proof of each setting.
- Missing, zero/false, wrong-type, out-of-range, conflicting, and invalid
  cross-parameter values have intentional behavior. Invalid parameters fail
  through existing config error reporting rather than a late internal assert.
- Float32, Float16, and BFloat16 size calculations are tested. Exercise vector
  arrays where supported, with unsupported combinations rejected explicitly.
- Static validation works without a runtime memory/CPU context.
- Default compressed AISAQ and explicit compressed DiskANN report no raw vectors;
  absolute `disk_pq_dims=0` retains exact retrieval. Load-time parameters cannot
  override the representation recorded in an existing artifact. Filtered search
  checks numerical distances through both cached and uncached refinement paths.

## PR 2: Milvus removes algorithm-specific parameter handling

Suggested title: `enhance: pass vector index parameters through to knowhere`

Changes:

- Bump the pinned Knowhere revision to PR 1.
- Remove `FillDiskIndexParams`, `UpdateDiskIndexBuildParams`,
  `SetDiskIndexBuildParams`, `SetDiskIndexLoadParams`, `BigDataIndexExtraParams`,
  and their parsing/call-site branches. Do not introduce `FillAiSAQIndexParams`.
- Remove the corresponding `common.DiskIndex.*` ParamItems, the special
  AutoIndex ExtraParams parser/configuration, shipped DiskANN/AISAQ YAML defaults,
  and parameter-specific ParamGroup documentation. Regenerate configuration
  output through the repository's normal generator.
- Simplify VectorDiskBuilder, VectorDiskLoader, and VectorIndexReader adaptation: remove thread-key
  translation, mandatory synthesized thread parameters, saved beamwidth state,
  unconditional search overrides, and algorithm-parameter resets. Keep generic
  file/data/resource plumbing.
- Ensure full load and Reopen share the same generic preparation path, without
  a duplicate enrichment pass or a disk-index-only prerequisite.
- Preserve index metadata separately from runtime load additions in the internal
  load payloads, including standalone admission estimates. Derive artifact
  capabilities from metadata and execution resource estimates from effective
  load settings.
- Delete the duplicate sparse algorithm whitelist and other directly affected,
  redundant algorithm parameter checks. Keep schema and function semantics.
- Keep explicit parameter maps intact across Proxy -> metadata -> build task
  and metadata -> load request. Do not add a second default-filling layer.
- Include the design document, configuration/API behavior notes, and all
  Milvus-side regression tests in this PR.

Acceptance:

- Through the real CreateIndex path, build/load/search both DiskANN and AISAQ
  with no algorithm configuration in Milvus YAML.
- Create two indexes with different explicit settings under one server config.
  Observe final consumer settings, including after release/load and Reopen;
  inspect the actual metadata and request inputs, not only helper output maps.
- Search requests with different beamwidth controls reach Knowhere unchanged.
  Cover omitted controls and both supported search and iterator paths.
- Invalid algorithm parameters are rejected through the existing validation
  path with appropriate errors; no new Go algorithm whitelist is needed.
- Exercise normal AutoIndex resolution as well as explicitly selected indexes,
  and smoke-test a non-disk index to catch generic pipeline regressions.
- Review all production writers of the affected keys, including blanket map
  merges, and trace each setting from request/default source to consumer.
- Verify returned vector fields against the original inserted values before and
  after release/load, including default compressed AISAQ. A successful search or
  capability boolean alone does not verify the field-data fallback.
- Run affected Go tests with `-tags dynamic,test -gcflags="all=-N -l" -count=1`,
  relevant C++ tests, and integration coverage. Apply repository-wide error
  contract checks if error projection or codes are changed.

## Delivery order and completion criterion

Implement and validate PR 1, then implement PR 2 against its exact revision.
Merge Knowhere first and Milvus second. No compatibility PR or separate test PR
is planned. This design does not depend on merging Milvus PR #50046.

Completion means a new Knowhere algorithm parameter can be introduced, defaulted,
validated, and used through Milvus without editing Milvus's algorithm parameter
handling or shipped YAML defaults. Passing helper tests alone does not meet it.
