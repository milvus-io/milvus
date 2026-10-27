# Offset foundation fixtures

These fixtures contain unchanged source records, not generated scalar values.
Request offset permutations/repeated requests in the tests do not increase the
number of dataset rows.

`cohere_original_ids_1024.json` retains its original VectorDBBench Cohere 1M
provenance, source hashes and row selection in the JSON metadata. It is used for
numeric expressions and transport/control tests, not ANN recall acceptance.

`nq_original_text_1024.json` selects the first 1,024 physical records of the
retained 830,152-row short-text snapshot from CohereLabs/beir-embed-english-v3,
revision `f018922a46a51388348a307ec2fef28019a33026`, `nq/corpus`. The original
source URLs, snapshot receipt/hash and upstream text attribution/license are
retained in the JSON; the C++ test's Apache license does not relicense that text.

The retained verification receipt directly attests original-source equality of
the ID/text pairs and hashes every captured title chunk. Titles are immutable
captured fields with the same physical row mapping; that receipt does **not**
contain an independent title-to-original-source equality digest. Do not describe
this as a fresh three-field source comparison or claim that the full source
parquet has been recovered. Tests compare title values to these frozen captured
bytes and use ID/text for independently source-matched predicate checks.

These fixtures have no original NULL records and do not establish nullable
collection or ANN end-to-end coverage. Larger immutable source/traces and
independent vector ground truth are required for those performance/quality
claims.
