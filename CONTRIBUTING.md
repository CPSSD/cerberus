Writing Code for Cerberus
=========================

## Code Style ##

Rust does not have an offical code style guide. We will use the style guide currently in the rust-lang-nursery. 
This style guide can be found [here](https://github.com/rust-lang-nursery/fmt-rfcs/blob/master/guide/guide.md).

## Code Formatting ##

All `rust` code checked into the repository must be formatted according to `rustfmt`.

## Commenting ##

Commenting will be done in accordance with the standard `rustdoc` style used for generating documentation. 
You should write comments to explain any code you write that does something in a way that may not be obvious.
An explaination of standard Rust commenting practice and `rustdoc` can be found [here](https://doc.rust-lang.org/book/first-edition/documentation.html).

## Automated Testing ##

Automated unit/integration tests should be written using the rust testing framework. 
Tests will be automatically ran for each pull request and pull requests will failling tests will not be merged.

## Branching ##

Branching should be done in accordance with the [git-flow](http://nvie.com/posts/a-successful-git-branching-model/) style of branching. 
There is a master branch and a develop branch. All features will be developed in feature branches off the develop branch.
Features will be merged into the develop branch when complete.
A release branch will be branched from the develop branch for final bugfixes before a release. When ready for a release it will be merged to the master branch.

## Merging ##
Before merging a branch into develop, all commits should be sqaushed to keep the history clean. 
This can be done using the merge and squash feature on github or the commits can be squashed locally.

## Version Numbering ##

We will be using [semantic versioning](http://semver.org/). Every binary will have a separate
version number (server, client, etc.). This will begin at 0.1.0 and will be incremented strictly
according to the semantic versioning guidelines.

## Code Review ##

All code must be submitted via pull requests, *not* checked straight into the repo.
A pull request will be from a single feature branch, which will not be used for any other
features after merging. Ideally, it will be deleted after a merge.

All pull requests must be reviewed by at least one person who is not the submitter. You can
ask in Slack for a review, or use Github's assign feature to assign the pull request to a
specific person.
