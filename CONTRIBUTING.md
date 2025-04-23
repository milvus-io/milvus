# Contributing to Milvus

Contributions to Milvus are welcome from everyone. We strive to make the contribution process simple and straightforward. Up-to-date information can be found at [milvus.io](https://milvus.io/).

The following are a set of guidelines for contributing to Milvus. Following these guidelines makes contributing to this project easy and transparent. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

As for everything else in the project, the contributions to Milvus are governed by our [Code of Conduct](CODE_OF_CONDUCT.md).

**Content**

- [Contributing to Milvus](#contributing-to-milvus)
  - [What contributions can you make?](#what-contributions-can-you-make)
  - [How can you contribute?](#how-can-you-contribute)
    - [Contributing code](#contributing-code)
    - [GitHub workflow](#github-workflow)
    - [General guidelines](#general-guidelines)
    - [Developer Certificate of Origin (DCO)](#developer-certificate-of-origin-dco)
  - [Coding Style](#coding-style)
    - [Golang coding style](#golang-coding-style)
    - [C++ coding style](#c-coding-style)
  - [Run unit test with code coverage](#run-unit-test-with-code-coverage)
    - [Golang](#run-golang-unit-tests)
    - [C++](#run-c-unit-tests)
  - [Commits and PRs](#commits-and-prs)

## What contributions can you make?

| Suitable for                             | Projects                                                                                                                                                                            | Resources                                                                                           |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Go developers                            | [milvus](https://github.com/milvus-io/milvus), [milvus-sdk-go](https://github.com/milvus-io/milvus-sdk-go)                                                                          |                                                                                                     |
| CPP developers                           | [milvus](https://github.com/milvus-io/milvus), [knowhere](https://github.com/milvus-io/knowhere)                                                                                    |                                                                                                     |
| Developers interested in other languages | [pymilvus](https://github.com/milvus-io/pymilvus), [milvus-sdk-node](https://github.com/milvus-io/milvus-sdk-node), [milvus-sdk-java](https://github.com/milvus-io/milvus-sdk-java) | [Contributing to PyMilvus](https://github.com/milvus-io/pymilvus/blob/master/CONTRIBUTING.md)       |
| Kubernetes enthusiasts                   | [milvus-helm](https://github.com/milvus-io/milvus-helm)                                                                                                                             |                                                                                                     |
| Tech writers and docs enthusiasts        | [milvus-docs](https://github.com/milvus-io/milvus-docs)                                                                                                                             | [Contributing to milvus docs](https://github.com/milvus-io/milvus-docs/blob/v2.0.0/CONTRIBUTING.md) |
| Web developers                           | [milvus-insight](https://github.com/zilliztech/milvus-insight)                                                                                                                      |                                                                                                     |

## How can you contribute?

### Contributing code

**If you encounter a bug, you can**

- (**Recommended**) File an issue about the bug.
- Provide clear and concrete ways/scripts to reproduce the bug.
- Provide possible solutions for the bug.
- Pull a request to fix the bug.

**If you're interested in existing issues, you can**

- (**Recommended**) Provide answers for issue labeled `question`.
- Provide help for issues labeled `bug`, `improvement`, and `enhancement` by
  - (**Recommended**) Ask questions, reproduce the issue, or provide solutions.
  - Pull a request to fix the issue.

**If you require a new feature or major enhancement, you can**

- (**Recommended**) File an issue about the feature/enhancement with reasons.
- Provide an MEP for the feature/enhancement.
- Pull a request to implement the MEP.

**If you are a reviewer/approver of Milvus, you can**

- Participate in [PR review](CODE_REVIEW.md) process.
- Instruct newcomers in the community to complete the PR process.

If you want to become a contributor of Milvus, submit your pull requests! For those just getting started, see [GitHub workflow](#github-workflow) below.

All submissions will be reviewed as quickly as possible.
There will be a reviewer to review the codes, and an approver to review everything aside the codes, see [code review](CODE_REVIEW.md) for details.
If everything is perfect, the reviewer will label `/lgtm`, and the approver will label `/approve`.
Once the 2 labels are on your PR, and all actions pass, your PR will be merged into base branch automatically by our @sre-ci-robot

### GitHub workflow

Generally, we follow the "fork-and-pull" Git workflow.

* [Fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) the [Milvus repo](https://github.com/milvus-io/milvus/tree/master) on GitHub.
* Clone your fork to your local machine with `git clone git@github.com:<yourname>/milvus.git`.
* Work in your local repo and file a PR. 

In your local repo:

1. [Configure](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/configuring-a-remote-repository-for-a-fork) your local repo by adding the remote official repo as upstream. 
2.  Then you can create a branch, make changes and [commit](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/committing-changes-to-a-pull-request-branch-created-from-a-fork).
3.  Lastly, fetch upstream (and resolve merge conflicts if necessary), rebase and push the changes to origin. You can submit a [pull request](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests) to get your code reviewed.
4.  Once getting approved, your code can be merged to `master`, yay!

Here is the process illustrated in details:
![](docs/developer_guides/figs/fork-and-pull.png)

Remember to [sync your forked repository](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo#keep-your-fork-synced) _before_ submitting proposed changes upstream. If you have an existing local repository, please update it before you start, to minimize the chance of merge conflicts.

```shell
git remote add upstream git@github.com:milvus-io/milvus.git
git fetch upstream
git checkout upstream/master -b my-topic-branch
```

![](docs/developer_guides/figs/local-develop-steps.png)

### General guidelines

Before submitting your pull requests for review, make sure that your changes are consistent with the [coding style](CONTRIBUTING.md#coding-style), and run [unit tests](CONTRIBUTING.md#run-unit-test-with-code-coverage) to check your code coverage rate.

- Include unit tests when you contribute new features, as they help to prove that your code works correctly, and also guard against future breaking changes to lower the maintenance cost.
- Bug fixes also require unit tests, because the presence of bugs usually indicates insufficient test coverage.
- Keep API compatibility in mind when you change code in Milvus. Reviewers of your pull request will comment on any API compatibility issues.
- When you contribute a new feature to Milvus, the maintenance burden is (by default) transferred to the Milvus team. This means that the benefit of the contribution must be compared against the cost of maintaining the feature.

### Developer Certificate of Origin (DCO)

All contributions to this project must be accompanied by acknowledgment of, and agreement to, the [Developer Certificate of Origin](https://developercertificate.org/). Acknowledgment of and agreement to the Developer Certificate of Origin _must_ be included in the comment section of each contribution and _must_ take the form of `Signed-off-by: {{Full Name}} <{{email address}}>` (without the `{}`). Contributions without this acknowledgment will be required to add it before being accepted. If contributors are unable or unwilling to agree to the Developer Certificate of Origin, their contribution will not be included.

Contributors sign-off that they adhere to DCO by adding the following Signed-off-by line to commit messages:

```text
This is my commit message

Signed-off-by: Random J Developer <random@developer.example.org>
```

Git also has a `-s` command line option to append this automatically to your commit message:

```shell
$ git commit -s -m 'This is my commit message'
```

## Coding Style

Keeping a consistent style for code, code comments, commit messages, and PR descriptions will greatly accelerate your PR review process.
We highly recommend you refer to and comply to the following style guides when you put together your pull requests:

### Golang coding style

- Coding style: refer to the [Effictive Go Style Guide](https://golang.org/doc/effective_go)

We also use `golangci-lint` to perform code check. Run the following command before submitting your pull request and make sure there is no issue reported:

```shell
$ make static-check
```

To format code

```shell
$ make fmt
```

### C++ coding style

The C++ coding style used in Milvus generally follows [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html).
We made the following changes based on the guide:

- 4 spaces for indentation
- Adopt .cpp file extension instead of .cc extension
- 120-character line length
- Camel-Cased file names

Install clang-format

```shell
$ sudo apt-get install clang-format
```

Check code style

```shell
$ make cppcheck
```

## Run unit test with code coverage

Before submitting your Pull Request, make sure you have run unit test, and your code coverage rate is >= 90%.

### Run golang unit tests

You can run all the Golang unit tests using make.

```shell
$ make test-go
```

You can also run unit tests in package level.

```shell
# run unit tests in datanode package
$ go test ./internal/datanode -cover
ok  	github.com/milvus-io/milvus/internal/datanode 3.874s	coverage: 88.2% of statements
```

You can run a sub unit test.

In this case, we are only concerned about the tests with name "SegmentReplica" and
sub tests with name "segmentFlushed". When running sub tests, the coverage is not concerned.

```shell
$ go test ./internal/datanode -run SegmentReplica/segmentFlushed
ok  	github.com/milvus-io/milvus/internal/datanode 0.019s
```

### Using mockery

It is recommended to use [Mockery](https://github.com/vektra/mockery) to generate mock implementations for unit test dependencies.

If your PR changes any interface definition, you shall run following commands to update all mockery implemented type before submitting it:

```shell
make generate-mockery
```

If your PR adds any new interface and related mockery types, please add a new entry under proper [Makefile](Makefile) `generate-mockery-xxx` command.

```Makefile
generate-mockery-xxx: getdeps
    # ...
    # other mockery generation commands
    # use mockery under $(INSTALL_PATH) to unify mockery binary version
    $(INSTALL_PATH)/mockery --name=NewInterface ...
```

### Run C++ unit tests

Install lcov

```shell
$ sudo apt-get install lcov
```

Run unit test and generate code for code coverage check

```shell
$ make codecov-cpp
```

## Commits and PRs

- Commit message and PR description style: refer to [good commit messages](https://chris.beams.io/posts/git-commit)
