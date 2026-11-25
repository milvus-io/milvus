# Analyzer security backport

This 2.6 branch has no ComputePhraseMatchSlop implementation, user-supplied
local dictionary files, or implemented file resource API. Those master/3.0
changes are inapplicable here; this backport does not introduce them.
Native analyzer creation errors no longer append the analyzer parameters.

Request-supplied gRPC tokenizer TLS file paths (`ca_cert`, `client_cert`,
`client_key`) are rejected before reading files or connecting.
