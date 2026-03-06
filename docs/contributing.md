# Contributing

Thank you for showing interest in contributing to Databasez.

Ways to help:

- try Databasez and [report bugs/issues](https://github.com/dymmond/databasez/issues/new)
- [implement features](https://github.com/dymmond/databasez/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
- [review pull requests](https://github.com/dymmond/databasez/pulls)
- improve documentation
- participate in discussions

## Reporting bugs and issues

The preferred flow starts with [GitHub Discussions](https://github.com/dymmond/databasez/discussions):

- potential bugs: raise as "Potential Issue"
- feature ideas: raise as "Ideas"

From there, we can escalate into a formal issue when appropriate.

When reporting, include:

- OS/platform
- Python version
- installed dependencies
- minimal reproducible snippet
- traceback/log output

## Development setup

Fork and clone:

```shell
$ git clone https://github.com/YOUR-USERNAME/databasez
$ cd databasez
```

Create environments:

```shell
$ hatch env create
```

## Running tests

```shell
$ hatch test
```

Pass extra pytest arguments after `--`:

```shell
$ hatch test -- tests/test_database_url.py -q
```

## Linting and formatting

```shell
$ hatch run ruff
$ hatch run lint
$ hatch run format
```

## Type checking

```shell
$ hatch run test:check_types
```

Taskfile shortcut:

```shell
$ task ruff
$ task ty
$ task lint
$ task format
```

## Documentation workflow (Zensical)

Prepare rendered docs (expands snippet includes):

```shell
$ hatch run docs:prepare
```

Build docs:

```shell
$ hatch run docs:build
```

Serve docs locally:

```shell
$ hatch run docs:serve
```

Taskfile shortcuts are also available:

```shell
$ task docs_prepare
$ task build
$ task serve
```

## Building package artifacts

```shell
$ hatch build
```

## Releasing (maintainers)

Before release:

- update changelog with user-visible changes
- bump version in `databasez/__init__.py`

Then create a GitHub release:

- title: `Version X.Y.Z`
- tag: `X.Y.Z`
- release body from changelog
