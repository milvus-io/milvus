# Regenerate the system configuration reference

Run `config-docs-generator` from the Milvus repository to prepare the English
reference pages for `milvus-io/milvus-docs`. Run it as a manual release
documentation step. The release workflows in this repository do not regenerate
or publish these pages.

Use `configs/milvus.yaml` from the exact Milvus release you are documenting. For
example, use `v3.0.5` for the `v3.0.x` docs branch when documenting Milvus 3.0.5,
and `v2.6.26` for the `v2.6.x` docs branch when documenting Milvus 2.6.26.

With the release tag available locally, run these commands from a Milvus
checkout containing the current generator:

```bash
(
  set -e
  release_tag=v3.0.5
  work_dir=$(mktemp -d)
  mkdir "$work_dir/pages"
  git show "${release_tag}:configs/milvus.yaml" > "$work_dir/milvus.yaml"
  go run ./cmd/tools/config-docs-generator -i "$work_dir/milvus.yaml" -o "$work_dir/pages"
  printf 'Generated reference pages in %s\n' "$work_dir/pages"
)
```

Review the generated `system_configuration.md` and `configure_*.md` files against
`site/en/reference/sys_config/` on the matching docs branch. Copy the reviewed
pages there and check for obsolete generated pages and links before submitting
the docs change. Preserve any separately maintained guidance during review.

The release YAML supplies the keys, descriptions, and values for the generated
tables. A key with no YAML comment has a blank description. The Default Value
column shows the value shipped in that file. It can differ from the fallback
used by Milvus when the key is absent. Parameters absent from the YAML, including
non-exported paramtable settings, are outside the generated reference.
