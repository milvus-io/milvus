# Milvus Code Review Guide

All PRs are checked in automatically by the sre-robot, with the following conditions:

1. DCO check passed
2. All test passed and code coverage check passed, with a `ci-passed` label
   - Notes: If there is a `[skip e2e]` tag in the commit message, it skips running e2e tests automatically,
     but it still runs UT tests and code checkers.
3. Reviewer passed, with a `/lgtm` label
4. Approver passed, with a `/approve` label

Generally speaking, reviewer is volunteered and can be anyone in the community who is familiar with the packages the PR modifies.
Reviewers are responsible for the logic correctness, error handling, unit test coverage and code readability.
While Approver focuses on overall design, code readability, and ensuring the PR follows code of
conduct(Such as meaningful title and commit message, marked with correct labels, meaningful comments). Currently,
all Approvers are listed under OWNERS_ALIASES file.

## Things to do before review

- Read the title, commit message and related issue of the PR, if it's not easy to understand, ask for improvement

- For a bug fix PR, there should be a detailed bug description in related issue, and make sure the test cases to cover this bug.

- For a function enhancement PR, understand the function use case, make sure the functionality is reasonable.

- For a performance PR, make sure benchmark result is listed in PR.

- Think deeply about why is the solution necessary, any workaround or substitutions?

## Things to check during the review

- Does the code follow style guide?

- Does the code do exactly the same as title and commit message describe?

- Can this function and variable's behavior be inferred by its name?

- Do unit tests cover all the important code branches?

- What about the edge cases and failure handling paths?

- Do we need better layering and abstraction?

- Are there enough comments to understand the intent of the code?

- Are hacks, workarounds and temporary fixes commented?

- If `[skip e2e]` is tagged, is it safe enough to skip running e2e tests?
  - Notes: it skips running e2e tests, if there is a `[skip e2e]` in the commit message.
- Does the code will generate similar logs many times in one second?

## Things to keep in mind when you are writing a review comment

- Be kind to the coder, not to the code.

- Ask questions rather than make statements.

- Treat people who know less than you with respect, deference, and patience.

- Remember to praise when the code quality exceeds your expectation.

- It isn't necessarily wrong if the coder's solution is different with yours.

- Community is not only about the product, it is about person. Help others to improve whenever possible.

## For Approvers

Besides All the reviewer's responsibility listed above, Approvers should also maintain code of conduct.

- Be sure the pr has only one commit, author has to do a squash commit in local REPO

- Commit message starts with a capital letter and does not end with punctuation

- Commit message is clear and meaningful. You can only have title without body if the title is self explained

- PR links to the correct issue, which clearly states the problems to be solved and the planned solution

- PR sets kind label

- The variable names appearing in the source code need to be readable. Comments are necessary if it is an unusual abbreviations

Thanks for [Code Review Guide](https://github.com/pingcap/tidb/blob/master/code_review_guide.md) from Pingcap community.
