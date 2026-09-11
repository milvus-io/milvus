# Local Code Check

The PR Code Check formats C++ with `clang-format-15` and then runs the C++
static checks. To reproduce the formatting part locally:

```bash
cd internal/core
CLANG_FORMAT=/opt/homebrew/Cellar/llvm@15/15.0.7/bin/clang-format \
  ./run_clang_format.sh "$PWD"
```

For a focused change, format only the changed `.cpp` and `.h` files with the
same binary. The CI failure message `The cpp files are not formatted` means
the formatting check failed; `clang-tidy not found` is an environment notice
in this local setup, not the reported failure.

Before pushing, also run:

```bash
git diff --check
make cppcheck
```

The full `build-ut-cov` job additionally runs the C++ unit tests and coverage;
passing local formatting/static checks does not replace that job.

Go formatting also runs `gofumpt`, not just `gofmt`. Use the repository's
`github.com/cockroachdb/errors` import in tests too; the standard `errors`
package is rejected by `depguard`. Reproduce these checks with the configured
`golangci-lint` version before treating Code Check as passed.
