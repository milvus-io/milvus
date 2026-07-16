# Query Traffic Routing Design

## 1. Goal

Query Traffic Routing adds a label-based routing policy before the existing
query load balancer.

It is used to express:

1. prefer local query-serving nodes;
2. fall back to other nodes when preferred candidates are unavailable;
3. split traffic between labeled groups by weight;
4. keep label meanings fully operator-defined.

Milvus does not assign built-in meanings to labels such as `AZ`,
`RESOURCE_GROUP`, `canary`, or `stable`.

## 2. Label Source

All server labels come from ETCD Session service discovery.

Query Traffic Routing does not depend on QueryCoord or shard leader metadata to
carry labels. Proxy resolves labels for itself and for candidate QueryNodes from
the Session discovery view.

Missing labels are treated as an empty label set.

## 3. Routing Position

Query Traffic Routing runs in Proxy after the normal candidate set has been
formed and unavailable nodes have been excluded.

It only filters or selects a candidate group. The existing load balancer still
chooses the final QueryNode inside that group.

```text
query candidates
        │
        ▼
resolve Session labels
        │
        ▼
apply queryTrafficRouting policy
        │
        ▼
filtered candidates
        │
        ▼
existing load balancer
```

## 4. Policy Model

A policy contains ordered rules. Rules are evaluated in order. A matching rule
is used only if its routes produce candidates. If it produces no candidates,
evaluation continues to the next matching rule. Fallback is expressed as an
ordinary lower-order rule.

Each matched rule contains routes:

1. evaluate routes against every candidate;
2. assign the route weight to each candidate node matched by that route;
3. pass the weighted candidate set to final load balancing.

`weight` is a per-candidate-node weight. If one route has `weight: 100` and
matches three QueryNodes, each of the three QueryNodes receives weight `100`.
The weight is not divided by the number of matched nodes.

Routes with `weight: 0` are kept in config but do not receive traffic and do not
make a rule usable.

If one candidate matches multiple routes in the same rule, the first matching
route wins. Candidate nodes that do not match any route in the selected rule are
not selected by that rule.

If no rule can produce candidates, routing falls back to the original candidate
set.

## 5. Matcher

The policy supports these label matchers:

| Matcher | Meaning |
|---|---|
| `any: true` | match all |
| `exists: ["K"]` | key exists |
| `not_exists: ["K"]` | key does not exist |
| `eq: {K: V}` | key equals value |
| `ne: {K: V}` | key does not equal value |
| `in: {K: [V1, V2]}` | key value is in the list |
| `not_in: {K: [V1, V2]}` | key value is not in the list |
| `match: {K: "regex"}` | key value matches the regular expression |
| `not_match: {K: "regex"}` | key value does not match the regular expression |

Multiple matcher fields in the same matcher are combined with AND.

Destination matchers may reference source labels:

```yaml
destinationLabels:
  eq:
    AZ: "${source.AZ}"
```

The reference must be the whole value. Partial string interpolation is not part
of the design.

## 6. Configuration

The config name is `proxy.queryTrafficRouting`.

Milvus ParamTable stores config as key-value strings. It does not currently
provide a subtree YAML reader such as `QueryTrafficRouting.GetAsYaml(&d)`.

The design uses two config items:

```text
proxy.queryTrafficRouting.enabled
proxy.queryTrafficRouting.rules
```

`rules` is written as a YAML array in config files and is read from ParamTable
as a JSON string.

Runtime update through `/management/config/alter` writes the same key-value
form. Since the REST value is a string, `rules` is passed as a compact JSON
string.

Example:

```json
{
  "configs": [
    {"key": "proxy.queryTrafficRouting.enabled", "value": "true"},
    {"key": "proxy.queryTrafficRouting.rules", "value": "[{\"name\":\"local-az\",\"routes\":[{\"name\":\"rg1\",\"weight\":100,\"destinationLabels\":{\"eq\":{\"AZ\":\"${source.AZ}\",\"RESOURCE_GROUP\":\"rg1\"}}}]}]"}
  ]
}
```

Omitting `value` resets the runtime override.

## 7. Timeline Example

Assume two AZs, `az1` and `az2`. Resource group names are independent from AZ
names. A resource group is selected only by its `RESOURCE_GROUP` label, and AZ
affinity is selected only by the `AZ` label.

### T0: Cross-AZ Affinity

During normal serving, the policy expresses cross-AZ affinity only. Local AZ
replicas are preferred. If no local candidate is available, the policy falls
back to any available candidate.

```yaml
proxy:
  queryTrafficRouting:
    enabled: true
    rules:
      - name: az-affinity
        match:
          sourceLabels:
            exists: ["AZ"]
        routes:
          - name: local
            weight: 100
            destinationLabels:
              eq: {AZ: "${source.AZ}"}

      - name: fallback
        match:
          sourceLabels:
            any: true
        routes:
          - name: any
            weight: 100
            destinationLabels:
              any: true
```

### T1: Add Replica by Resource Group

First add `rg3-0` into the routing config with `weight: 0`. The route exists,
but it does not receive traffic before the replica is ready.

```yaml
proxy:
  queryTrafficRouting:
    enabled: true
    rules:
      - name: az-affinity-with-rg
        match:
          sourceLabels:
            exists: ["AZ"]
        routes:
          - name: other-local-rgs
            weight: 100
            destinationLabels:
              eq: {AZ: "${source.AZ}"}
              ne: {RESOURCE_GROUP: "rg3-0"}
          - name: rg3-0
            weight: 0
            destinationLabels:
              eq: {AZ: "${source.AZ}", RESOURCE_GROUP: "rg3-0"}

      - name: fallback
        match:
          sourceLabels:
            any: true
        routes:
          - name: any
            weight: 100
            destinationLabels:
              any: true
```

- Expand replicas from `rg1-0, rg2-1` to `rg1-0, rg2-1, rg3-0`.
- Start gray traffic release after `rg3-0` is ready and observe stability during
  each step:

```text
rg1-0:100, rg2-1:100, rg3-0:0
--- rg3-0 ready ---
rg1-0:100, rg2-1:100, rg3-0:10
rg1-0:100, rg2-1:100, rg3-0:20
...
rg1-0:100, rg2-1:100, rg3-0:100
--- Done ---
```

- After traffic release is complete, restore the route config to the AZ
  affinity policy.

### T2: Replica Rolling Inside Resource Groups

First add `rg1-1` into the routing config with `weight: 0`. Keep `rg1-0` and
other local resource groups at `weight: 100`, so the route exists without
changing existing traffic distribution.

```yaml
proxy:
  queryTrafficRouting:
    enabled: true
    rules:
      - name: az-affinity-with-rg
        match:
          sourceLabels:
            exists: ["AZ"]
        routes:
          - name: rg1-0
            weight: 100
            destinationLabels:
              eq: {AZ: "${source.AZ}", RESOURCE_GROUP: "rg1-0"}
          - name: rg1-1
            weight: 0
            destinationLabels:
              eq: {AZ: "${source.AZ}", RESOURCE_GROUP: "rg1-1"}
          - name: other-local-rgs
            weight: 100
            destinationLabels:
              eq: {AZ: "${source.AZ}"}
              not_in:
                RESOURCE_GROUP: ["rg1-0", "rg1-1"]

      - name: fallback
        match:
          sourceLabels:
            any: true
        routes:
          - name: any
            weight: 100
            destinationLabels:
              any: true
```

- Expand replicas from `rg1-0, rg2-1` to `rg1-0, rg1-1, rg2-1`.
- Shift traffic by changing only weights and observe stability during each step:

```text
rg1-0:100, rg1-1:0,   rg2-1:100
--- rg1-1 ready ---
rg1-0:90,  rg1-1:10,  rg2-1:100
...
rg1-0:0,   rg1-1:100, rg2-1:100
--- Done ---
```

- After traffic has drained from `rg1-0`, reclaim the `rg1-0` replica and
  restore the route config to the AZ affinity policy.

### T3: AZ Changes During Rolling

If the rolling replica is placed in another AZ, no special routing config is
needed. Reuse the T2 config. Routes still keep `AZ: "${source.AZ}"`, so
non-rolling traffic remains pinned to the local AZ.

AZ changes are reflected by Session labels:

1. when the old replica still exists in the current AZ, current-AZ traffic keeps
   hitting the old replica;
2. the new replica starts receiving traffic from the AZ where it is located;
3. after the old replica weight is drained to `0`, traffic naturally moves to
   the AZ where the new replica is located.

The routing policy does not explicitly write `AZ: "az1"` or `AZ: "az2"` during
rolling.

### T4: Scale Down Replica by Resource Group

For scale-down, first put the target replica in the route config and drain its
weight to `0`. For example, scale replicas from `rg1-0, rg2-1, rg3-0` back to
`rg1-0, rg2-1`:

```yaml
proxy:
  queryTrafficRouting:
    enabled: true
    rules:
      - name: az-affinity-with-rg
        match:
          sourceLabels:
            exists: ["AZ"]
        routes:
          - name: other-local-rgs
            weight: 100
            destinationLabels:
              eq: {AZ: "${source.AZ}"}
              ne: {RESOURCE_GROUP: "rg3-0"}
          - name: rg3-0
            weight: 100
            destinationLabels:
              eq: {AZ: "${source.AZ}", RESOURCE_GROUP: "rg3-0"}

      - name: fallback
        match:
          sourceLabels:
            any: true
        routes:
          - name: any
            weight: 100
            destinationLabels:
              any: true
```

- Drain `rg3-0` by changing only its weight:

```text
rg1-0:100, rg2-1:100, rg3-0:100
rg1-0:100, rg2-1:100, rg3-0:50
rg1-0:100, rg2-1:100, rg3-0:10
rg1-0:100, rg2-1:100, rg3-0:0
--- Done ---
```

- After `rg3-0` has no traffic, reclaim the `rg3-0` replica.
- After scale-down is complete, restore the route config to the AZ affinity
  policy.

## 8. Fallback and Compatibility

Query availability has priority over strict policy enforcement.

Fallback should normally be expressed as the last `any: true` rule. When a
matched rule cannot produce candidates, the engine continues evaluating later
rules.

The router falls back to the original candidate set only in these cases:

1. the feature is disabled;
2. no rule matches;
3. no matching rule can produce candidates;
4. Session labels cannot be resolved.

The feature is disabled by default. When disabled, query routing behavior is
unchanged.

## 9. Observability

Routing decisions expose only bounded fields used to evaluate rule hit counts
and downstream request distribution:

1. selected rule name;
2. selected route name;
3. fallback reason;
4. input candidate count;
5. selected candidate count.

Arbitrary Session label keys and values must not be used as metric labels.
