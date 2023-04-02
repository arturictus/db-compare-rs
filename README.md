# DB-Compare

Command line tool to compare two postgres databases.

Useful to verify when migrating databases.

## Installation

```shell
cargo install --git "https://github.com/arturictus/db-compare-rs"
```

## Usage

```sh
db-compare --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```

Run `--help` for more information

```sh
db-compare --help
```

### Jobs

- **by_id_excluding_replica_updated_ats** [default if no job list is supplied]

  _it tries to compare grouping by id if the table has the id column_

  1. Gets replica `updated_at` `id` after cutoff.
  2. Gets max number `id` from the master.
  3. Compares rows by id excluding ids in the cutoff updated at the list.
  4. Stops if `by-id-sample-size` arg is reached.

- **by_id**

  _it tries to compare grouping by id if the table has the id column_

  1. Gets max number `id` from the master.
  2. Compares rows grouped by id.
  3. Stops if `by-id-sample-size` arg is reached.

- **counters**

  Compares the `count` of each table between databases.

- **sequences**

  Compares the `sequences` of each table between databases.

- **last_updated_ats:**

  _it tries to compare grouping by id if the table has the id column_

  Compares last updated ats rows until the `limit` arg is reached

- **last_created_ats:**

  _it tries to compare grouping by id if the table has the id column._

  Compares last created ats rows until the `limit` arg is reached

### Output

Markers:

- `-`: Exists in Master but not in Replica.
- `+`: Exists in Replica but not in Master.
- `>`: Exists in both but with differences.
- `@@ ... @@`: Comments, helpful to split into different files or the see context of the query.

### Config File

You can pass all the arguments in a `yaml` file for convenience.

**IMPORTANT:** Command params take precedence over this configuration.

```sh
db-compare --config ./config.yml
```

```yaml
# ./config.yml
db1: "postgresql://postgres:postgres@127.0.0.1/db1"
db2: "postgresql://postgres:postgres@127.0.0.1/db2"
tables:
  - testing_tables
jobs:
  - by_id_excluding_replica_updated_ats # default: no need to pass jobs list if only running this job
  - counters
  - last_updated_ats
  - last_created_ats
  - by_id
  - sequences
  - updated_ats_until
limit: 100
diff-file: ./diff_from_testing.diff
by-id-sample-size: 10000 # If wanting to test all rows, remove this config
```

Most of the configs can be overridden by command parameters.

```sh
db-compare --db2 postgresql://postgres:postgres@127.0.0.1/another_replica --limit 100 --diff-file ./tmp/another_replica.diff
```

## Development

```sh
cargo run -- --db1 postgresql://postgres:postgres@127.0.0.1/my_database --db2 postgresql://postgres:postgres@[other]/my_database
```
