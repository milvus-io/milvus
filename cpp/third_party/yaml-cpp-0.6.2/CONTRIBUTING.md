# Style

This project is formatted with [clang-format][fmt] using the style file at the root of the repository. Please run clang-format before sending a pull request.

In general, try to follow the style of surrounding code. We mostly follow the [Google C++ style guide][cpp-style].

Commit messages should be in the imperative mood, as described in the [Git contributing file][git-contrib]:

> Describe your changes in imperative mood, e.g. "make xyzzy do frotz"
> instead of "[This patch] makes xyzzy do frotz" or "[I] changed xyzzy
> to do frotz", as if you are giving orders to the codebase to change
> its behaviour.

[fmt]: http://clang.llvm.org/docs/ClangFormat.html
[cpp-style]: https://google.github.io/styleguide/cppguide.html
[git-contrib]: http://git.kernel.org/cgit/git/git.git/tree/Documentation/SubmittingPatches?id=HEAD

# Tests

Please verify the tests pass by running the target `tests/run_tests`.

If you are adding functionality, add tests accordingly.

# Pull request process

Every pull request undergoes a code review. Unfortunately, github's code review process isn't great, but we'll manage. During the code review, if you make changes, add new commits to the pull request for each change. Once the code review is complete, rebase against the master branch and squash into a single commit.
