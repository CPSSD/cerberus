Writing Code for Cerberus
=========================

## Code Style ##

TODO: Decide on code style guidelines and add them here.

## Code Formatting ##

All `rust` code checked into the repository must be formatted according to `rustfmt`.

## Commenting ##

TODO: Write commenting instructions

## Automated Testing ##

TODO: Write automated testing instructions

## Branching ##

Branches should have a short prefix describing what type of changes are contained in it.
These prefixes should be one of the following:

* **feature/** -- for changes which add/affect a feature.
* **doc/** -- for changes to the documentation.
* **hotfix/** -- for quick bugfixes.

Branches must also contain the name of the contributor, i.e. `doc/terry/...`.

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
