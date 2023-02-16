# DB-Compare

Command line tool to compare two postgres databases.

Usefull to verify when migrating databases.

## Installation

- get github token to access to repo releases
- install [eget](https://github.com/zyedidia/eget)

**run:**

```sh
export EGET_GITHUB_TOKEN=ghp_1234567890

eget --sha256 selma-finance/rust_db_migrate_checker
=> sha256output

eget --verify-sha256=sha256output selma-finance/rust_db_migrate_checker
```

## RUN

```sh
db-compare --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```

Run `--help` for more information

```sh
db-compare --help
```

### Config File

You can pass all the arguments in a yaml file for convenience.

```sh
db-compare --config ./config.yml
```

_./config.yml_

```yaml
db1: "postgresql://postgres:postgres@127.0.0.1/db1"
db2: "postgresql://postgres:postgres@127.0.0.1/db2"
tables:
  - testing_tables
jobs:
  - counters
  - last_updated_ats
  - last_created_ats
  - all_columns
limit: 100
diff-file: ./diff_from_testing.diff
all-columns-sample-size: 10000
```

All configs can be overriden by command params.

```sh
db-compare --db2 postgresql://postgres:postgres@127.0.0.1/another_replica --limit 100 --diff-file ./tmp/another_replica.diff
```

## Development

```sh
cargo run -- --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```
