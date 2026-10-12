# Raw String Literals for Filter Expressions

update: 6.26.2026

issue: [#43864](https://github.com/milvus-io/milvus/issues/43864)

## Motivation

A backslash in a `LIKE` pattern (or a regex, or any string value) currently has
to survive **two** unescaping layers inside Milvus before it reaches the matcher:

1. **String-literal layer** — the expression parser treats a double-quoted /
   single-quoted string as a C-style literal and runs `strconv.Unquote`
   (`\\`→`\`, `\n`→newline, `\"`→`"`, `\uXXXX`→rune). See
   `convertEscapeSingle` in `internal/parser/planparserv2/utils.go`.
2. **Pattern layer** — `LIKE` then applies its own escape rules (`\%`→`%`,
   `\_`→`_`, `\\`→`\`); regex applies regex escaping.

Each layer halves the number of backslashes, so matching a single literal `\`
requires `"\\\\"` (4 backslashes) at the expression level — and 8 once the
client language (e.g. Python) adds its own layer. This is the core complaint of
issue #43864.

PostgreSQL avoids the extra layer because, with `standard_conforming_strings`
(on by default), `\` is **not** special inside ordinary string literals — only
the pattern layer processes it. Milvus's expression string layer behaves like
MySQL's default (C-style escapes).

## Goals

- Let users write filter strings where `\` is taken **verbatim**, removing the
  string-literal unescaping layer.
- Do it **without breaking** any existing query: ordinary `"..."` / `'...'`
  literals keep their current C-style behavior.
- Align with prior art rather than inventing semantics.

## Non-goals

- Changing the default behavior of ordinary string literals (the breaking
  `standard_conforming_strings` switch is left for a separate, gated effort).
- Changing the `LIKE` / regex pattern layer itself (covered separately by the
  escape-model fix for issue #43864).

## Prior art

Raw string literals are a well-established database feature:

- **BigQuery / Spark SQL**: `r"..."` / `R'...'` — a backslash is a literal
  character. BigQuery's `LIKE` docs explicitly state that with raw strings only
  a single backslash is needed, e.g. `r'\%'`.
- **PostgreSQL / Snowflake / DuckDB**: dollar-quoting `$$...$$` (fully raw); PG
  default makes `\` literal in ordinary strings, with `E'...'` for escapes.
- **Programming languages**: Python `r"..."`, Go `` `...` ``, C# `@"..."`,
  Rust `r#"..."#`.

This design follows the BigQuery / Spark `r"..."` form.

## Design

### Syntax

```
RawStringLiteral: [rR] ( '"' DoubleRChar* '"' | '\'' SingleRChar* '\'' );
```

An `r` or `R` immediately preceding the opening quote marks a raw string. ANTLR
maximal-munch makes `r"..."` lex as one raw token rather than identifier `r`
followed by a string; a bare `r` is still an ordinary identifier.

Inside a raw string the backslash is **not** an escape character. A backslash
before the closing delimiter only prevents termination (the backslash and the
quote both stay in the value), so — like Python — a raw string cannot end with
an odd number of backslashes. To embed the delimiter quote, use the other quote
style (`r'a"b'`).

### Semantics

The content between the quotes becomes the string value verbatim — no
`strconv.Unquote`. The downstream pattern layer is unchanged, so a raw string in
a `LIKE` still goes through `LIKE` escaping:

| Expression            | Value seen by matcher | Result            |
|-----------------------|-----------------------|-------------------|
| `A == r"a\b"`         | `a\b`                 | equals `a\b`      |
| `A like r"\%"`        | `\%` → `%`            | equals `%`        |
| `A like r"\\%"`       | `\\` → `\`            | prefix `\`        |
| `A like r"a\\b"`      | `a\\` → `a\`          | equals `a\b`      |
| `A =~ r"\d+"`         | `\d+`                 | regex `\d+`       |

A literal backslash in `LIKE` now needs `r"\\"` (2) instead of `"\\\\"` (4),
matching PostgreSQL's expression-level count. (The remaining client-language
layer, e.g. Python, is unaffected and avoidable with the client's own raw
strings.)

### Implementation

- `Plan.g4`: add the `RawStringLiteral` lexer token, the `DoubleRChar` /
  `SingleRChar` fragments, a `# RawString` alternative in `expr`, and accept
  `RawStringLiteral` inside the `JSONIdentifier` `[...]` subscript. Regenerate
  the Go parser via `generate.sh` (ANTLR 4.13.2, Go target only — the C++ side
  executes the serialized plan and needs no regeneration).
- `parser_visitor.go`: `VisitRawString` strips the prefix + quotes and emits the
  content verbatim as a `VarChar` value; `parseRegexPatternOrTemplate` accepts a
  raw token as a verbatim regex pattern. The general value path means raw
  strings work for `==`, `IN`, `LIKE`, `=~`, JSON value comparisons, etc.
- JSON **path keys**: `getColumnInfoFromJSONIdentifier` drops the `r`/`R` prefix
  of a raw subscript key. JSON keys are already taken verbatim (no `Unquote`
  pass), so `JSONField[r"a\b"]` is equivalent to `JSONField["a\b"]` — the raw
  form is accepted purely so `r"..."` works everywhere a string literal can
  appear, instead of being a parse error in JSON paths.

## Compatibility

Purely additive. Existing `"..."` / `'...'` literals are untouched. The only new
surface is the `r`/`R` prefix before a quote, which previously was a syntax
error. SDKs may expose the syntax but need no change to keep working.

## Testing

`TestExpr_RawString` in `plan_parser_v2_test.go` covers verbatim `==` values,
`LIKE` (literal `%`/`_`/`\`, prefix/inner), single- and double-quoted raw
strings, the raw-vs-normal backslash-count equivalence, and raw regex.
