# Changelog

## 0.1.0 (2026-08-16)


### Features

* attach kubectl plugin binaries to releases ([15ed958](https://github.com/rogeeoh/soft-drain/commit/15ed958e1da0f63fb7cffbee4129af12339a7601))
* close out the MVP — version subcommand, automation contract and RBAC docs ([92e3667](https://github.com/rogeeoh/soft-drain/commit/92e366707270a17f04beee8b202ab224880ea55a))
* finish the kubectl plugin — status and release subcommands ([77024f5](https://github.com/rogeeoh/soft-drain/commit/77024f5bf14d6b3e6e0dfbba597a44144a3bd3b7))
* kubectl plugin, soft-drain.com label prefix, English README ([cfc5076](https://github.com/rogeeoh/soft-drain/commit/cfc5076ac0e8cbdd32dac8ce9664d682b861fafd))
* placement and reclamation controllers, review round one applied ([85bcef0](https://github.com/rogeeoh/soft-drain/commit/85bcef077bef8d3c767ec06b739d90f6f5f27aff))
* plugin accepts multiple nodes — like kubectl drain ([0e40a6a](https://github.com/rogeeoh/soft-drain/commit/0e40a6a963e1c49ce7a116684c517ffca095051e))
* release-please and ghcr release pipeline, Helm chart ([af0482b](https://github.com/rogeeoh/soft-drain/commit/af0482b12c68a9799ab1dd749b2e99ae77b5d736))
* uncordon ends involvement regardless of state — folds even after Complete ([2398b26](https://github.com/rogeeoh/soft-drain/commit/2398b2690da88ed9770342a5d098c8065e6fa31a))


### Bug Fixes

* off-by-one that skipped the first arg in the completion script ([641d746](https://github.com/rogeeoh/soft-drain/commit/641d7463533151639b30fa1c5b16fa3d090c5553))
* treat Ctrl-C as detaching, not failing — friendly notice, exit 130 ([3b93237](https://github.com/rogeeoh/soft-drain/commit/3b932379b3255a3d648516c7a1ba95b932bd05dc))


### Documentation

* state the origin — born operating on-premise GPU clusters, GPU-specific in nothing ([5edab54](https://github.com/rogeeoh/soft-drain/commit/5edab542b136def9e4efe3b5d90bad0c28210416))
