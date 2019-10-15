# Contributing to Milvus

First of all, thanks for taking the time to contribute to Milvus! It's people like you that help Milvus come to fruition.

The following are a set of guidelines for contributing to Milvus. Following these guidelines helps contributing to this project easy and transparent. These are mostly guideline, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

As for everything else in the project, the contributions to Milvus are governed by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Contribution Checklist

Before you make any contributions, make sure you follow this list.

- Read [Contributing to Milvus](CONTRIBUTING.md).
- Check if the changes are consistent with the [coding style](CONTRIBUTING.md#coding-style).
- Run [unit tests](CONTRIBUTING.md#run-unit-test).

## What contributions can I make?

Contributions to Milvus fall into the following categories.

1. To report a bug or a problem with documentation, please file an [issue](https://github.com/milvus-io/milvus/issues/new/choose) providing the details of the problem. If you believe the issue needs priority attention, please comment on the issue to notify the team.
2. To propose a new feature, please file a new feature request [issue](https://github.com/milvus-io/milvus/issues/new/choose). Describe the intended feature and discuss the design and implementation with the team and community. Once the team agrees that the plan looks good, go ahead and implement it, following the [Contributing code](CONTRIBUTING.md#contributing-code).
3. To implement a feature or bug-fix for an existing outstanding issue, follow the [Contributing code](CONTRIBUTING.md#contributing-code). If you need more context on a particular issue, comment on the issue to let people know.

## How can I contribute?

### Contributing code

If you have improvements to Milvus, send us your pull requests! For those just getting started, GitHub has a [how-to](https://help.github.com/en/articles/about-pull-requests).

The Milvus team members will review your pull requests, and once it is accepted, it will be given a `ready to merge` label. This means we are working on submitting your pull request to the internal repository. After the change has been submitted internally, your pull request will be merged automatically on GitHub.

### General guidelines

Before sending your pull requests for review, make sure your changes are consistent with the guidelines and follow the Milvus coding style.

- Include unit tests when you contribute new features, as they help to prove that your code works correctly, and also guard against future breaking changes to lower the maintenance cost.
- Bug fixes also require unit tests, because the presence of bugs usually indicates insufficient test coverage.
- Keep API compatibility in mind when you change code in Milvus. Reviewers of your pull request will comment on any API compatibility issues.
- When you contribute a new feature to Milvus, the maintenance burden is (by default) transferred to the Milvus team. This means that the benefit of the contribution must be compared against the cost of maintaining the feature.


## Coding Style
The coding style used in Milvus generally follow [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html).
And we made the following changes based on the guide:

- 4 spaces for indentation
- Adopt .cpp file extension instead of .cc extension
- 120-character line length
- Camel-Cased file names


## Run unit test

We use Google Test framework for test running.
To run unit test for Milvus under C++, please use the following command:

```shell
# Run unit test for Milvus
$ ./build.sh -u
```


