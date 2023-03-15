# DB-Compare

Command line tool to compare two postgres databases.

Usefull to verify when migrating databases.

## Installation

- install [eget](https://github.com/zyedidia/eget)

**run:**

```sh

eget --sha256 arturictus/db-compare-rs
=> sha256output

eget --verify-sha256=sha256output arturictus/db-compare-rs
```

## Usage

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
  - sequences
  - updated_ats_until
limit: 100
diff-file: ./diff_from_testing.diff
all-columns-sample-size: 10000
```

Most of the configs can be overriden by command params.

```sh
db-compare --db2 postgresql://postgres:postgres@127.0.0.1/another_replica --limit 100 --diff-file ./tmp/another_replica.diff
```

## Development

```sh
cargo run -- --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```
