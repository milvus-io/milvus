# Milvus repository documentation

This directory contains documentation that must stay close to the Milvus source
tree. Public product documentation belongs in the
[`milvus-io/milvus-docs`](https://github.com/milvus-io/milvus-docs) repository.

## Directory contract

| Path | Purpose | Current or historical? |
| --- | --- | --- |
| [`agent_guides/`](agent_guides/) | Code-backed subsystem architecture, invariants, and debugging guidance | Current |
| [`dev/`](dev/) | Contributor-facing engineering rules and development procedures | Current |
| [`design-docs/`](design-docs/) | Formal design decisions and their supporting assets | Historical record |
| [`user_guides/`](user_guides/) | Temporary staging for user documentation not yet published in `milvus-docs` | Current, temporary |
| [`archive/`](archive/) | Superseded documentation retained only for historical context | Historical, non-normative |

Documentation outside these paths should be moved into the appropriate class or
kept next to the code it documents. Ignored local planning directories, such as
`docs/plans/`, must not be referenced by tracked code or documentation.

## Lifecycle rules

- Current documentation must be updated in the same pull request as the code or
  workflow whose behavior it describes.
- Design documents record the decision made at review time. Do not rewrite old
  documents to pretend they described the current implementation; add a status
  note, a follow-up design document, or update current architecture guidance
  instead.
- Public user documentation should be published in `milvus-docs`. Content under
  `user_guides/` is a temporary exception and should be removed after it is
  published there.
- Archived documentation is not a source of current Milvus behavior. New
  documentation must not be added under `archive/`.
- Images and other assets must have a tracked consumer. Prefer assets local to
  the documentation module that owns them, and avoid committing original files
  that contain unnecessary metadata or are substantially larger than their
  rendered form.

## Formal design documents

A formal design document is any Markdown file under
`docs/design-docs/design_docs/`, including documents in legacy or topic
subdirectories. New documents should use the recommended
`YYYYMMDD-short-descriptive-name.md` filename, but historical names do not
remove a document from the Design Doc review policy. User guides, reference
manuals, and experimental source code should live outside `design_docs/`.
Keep executable experiments with the implementation, tests, or benchmark
suite they validate.

See [`design-docs/README.md`](design-docs/README.md) for the recommended review
metadata, approval policy, and template.

## Validation

Scheduled and manually triggered checks validate deterministic local documentation links. Design-document
pull requests also receive automated classification, advisory metadata
reminders, and the additional approval policy described in the design-document
guide.
