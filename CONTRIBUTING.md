Writing Code for Cerberus
=========================

## Code Style ##

- Rust does not have an official code style guide. We will use the style guide currently in the
  [rust-lang-nursery](https://github.com/rust-lang-nursery/fmt-rfcs/blob/master/guide/guide.md).
- Protobufs should use our own 100 character line limit.
- Markdown files should follow sane styleguide, such as the one in [carwin/markdown-styleguide](https://github.com/carwin/markdown-styleguide),
  with an exception of the line char limit, which is a 100.

## Code Formatting ##

All `rust` code checked into the repository must be formatted according to `rustfmt`.

## Commenting ##

Commenting will be done in accordance with the standard `rustdoc` style used for generating
documentation. You should write comments to explain any code you write that does something in a
way that may not be obvious. An explanation of standard Rust commenting practice and `rustdoc` can
be found in [The Rust Programming Language book](https://doc.rust-lang.org/book/first-edition/documentation.html).

## Automated Testing ##

Automated unit/integration tests should be written using the rust testing framework. Tests will be
automatically ran for each pull request and pull requests with failling tests will not be merged.

## Branching ##

Branching should be done in accordance with the [git-flow](http://nvie.com/posts/a-successful-git-branching-model/)
style of branching. There is a master branch and a develop branch. All features will be developed
in feature branches off the develop branch. Features will be merged into the develop branch when
complete. A release branch will be branched from the develop branch for final bugfixes before
a release. When ready for a release it will be merged to the master branch.

## Merging ##

Before merging a branch into develop, all commits should be squashed to keep the history clean.
This should be done locally as the squash feature on github uses a fast forward merge.

## Version Numbering ##

We will be using [semantic versioning](http://semver.org/). Every binary will have a separate
version number (server, client, etc.). This will begin at 0.1.0 and will be incremented strictly
according to the semantic versioning guidelines.

## Code Review ##

All code must be submitted via pull requests, *not* checked straight into the repository. A pull
request will be from a single feature branch, which will not be used for any other features after
merging. Ideally, it will be deleted after a merge.

All pull requests must be reviewed by at least one person who is not the submitter. You can ask
in Slack for a review, or use Github's assign feature to assign the pull request to a specific
person.
