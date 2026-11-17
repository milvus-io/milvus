# Aliyun credentials compatibility copy

Source: https://github.com/aliyun/credentials-go/tree/v1.2.7/credentials
License: Apache-2.0; the upstream LICENSE is retained alongside this file.

The 21 production Go files under credentials/ were copied from version1.2.7;
upstream tests, test tokens, keys, root examples and module files were excluded.
Internal self-imports point into this package. Selection precedence and supported
configuration/profile modes remain the upstream versions used by Milvus.

Reason: the upstream public Credential interface exposes individual getters but
not the authoritative expiration or an atomic tuple. Its refreshers also publish
new session fields before checking expiration parse errors. An adapter cannot
safely recover that lifetime using public APIs, and a module replace would not
propagate to downstream consumers of Milvus/pkg.

Local changes:
- validate complete candidate fields and actual future expiration before publish;
- retain and serve old session credentials only until their real expiration;
- serialize refresh/snapshot with the updater mutex;
- add RetrieveSnapshot, returning an atomic tuple, absolute expiration and
  explicit static-mode indicator (RSA session keys intentionally have no token);
- remove credential bodies/values from debug logs and error messages;
- use typed local credentialError values, translated to merr at the adapter;
- keep the selected OIDC token path fixed across refresh failures;
- add loopback HTTP regression coverage for every refreshing provider.

Keep these changes isolated. Revisit the copy when upstream offers the required
atomic expiration-bearing API and equivalent refresh-failure guarantees.
