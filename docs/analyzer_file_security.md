# Analyzer security backport

This 2.6 branch has no ComputePhraseMatchSlop implementation, user-supplied
local dictionary files, or implemented file resource API. Those master/3.0
changes are inapplicable here; this backport does not introduce them.
Native analyzer creation errors no longer append the analyzer parameters.

Request-supplied gRPC tokenizer TLS file paths (`ca_cert`, `client_cert`,
`client_key`) must stay under the startup-only `tls.allowedPath` directory,
which defaults to `<localStorage.path>/tls`. Relative paths resolve under that
base; absolute paths must be descendants. Reads reject traversal, symlinks and
non-regular files and enforce a 1 MiB limit per file without echoing contents.
Only provision credentials intended for this consumer: requests can select files
inside the directory. See [TLS file security](agent_guides/analyzer_tls_security.md)
for the full deployment requirements.
